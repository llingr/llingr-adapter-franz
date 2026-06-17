// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Compile-time verification of interface compliance.
var _ nexus.BrokerPort[*kgo.Record] = (*Adapter)(nil)
var _ nexus.BandwidthPort[*kgo.Record] = (*Adapter)(nil)

// Subscribe to topic. With franz-go, topic subscription is typically done
// at client creation via kgo.ConsumeTopics(), so this validates readiness.
// Topic name is provided at adapter construction, not here.
func (a *Adapter) Subscribe() error {
	a.logger.Info(a.ctx, fmt.Sprintf("franz-go subscribed to topic: %s", a.topicName))
	return nil
}

// Unsubscribe and close the client.
func (a *Adapter) Unsubscribe() error {
	if a.bwCollector != nil {
		a.bwCollector.stop()
	}
	if a.closeFn != nil {
		a.closeFn()
	}
	return nil
}

// Poll fetches the next record from franz-go.
//
// Uses BlockRebalanceOnPoll pattern - rebalances are blocked during poll,
// and AllowRebalance() is called after processing to permit them.
func (a *Adapter) Poll(timeout time.Duration) (*kgo.Record, bool, error) {
	if a.pollRecordsFn == nil {
		return nil, false, fmt.Errorf("client closed")
	}

	ctx, cancel := context.WithTimeout(a.ctx, timeout)
	defer cancel()

	fetches := a.pollRecordsFn(ctx, 1)

	if fetches.IsClientClosed() {
		return nil, false, fmt.Errorf("client closed")
	}

	if a.allowRebalanceFn != nil {
		a.allowRebalanceFn()
	}

	if err := a.processPendingRebalances(); err != nil {
		return nil, false, err
	}

	// Deliver a record from a healthy partition first; a single failing
	// partition must not starve the others.
	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if record == nil {
			record = r
		}
	})

	// Rate-limit-log per-partition fetch errors and bail if one persists.
	// Errors are absorbed (not returned) so the consumer's poll loop does not
	// re-log them on every poll.
	sawErr := a.handlePollErrors(fetches)

	if record != nil {
		return record, true, nil
	}

	// Broker error with no record: back off briefly so the loop does not spin on
	// a broker that returns buffered errors immediately. Context-aware, and
	// skipped when the backoff is disabled (0).
	if sawErr && a.opts.pollErrorBackoff > 0 {
		select {
		case <-time.After(a.opts.pollErrorBackoff):
		case <-a.ctx.Done():
		}
	}
	return nil, false, nil
}

// handlePollErrors logs per-partition fetch errors and bails the consumer once
// a partition has been failing for PollErrorBailAfter. Timeout errors are
// ignored, and errors are absorbed rather than returned so the poll loop does
// not re-log them every poll. Logging is throttled per partition to once per
// PollErrorLogInterval, except a changed error (by errIdentity) logs at once.
// Returns whether a real (non-timeout, non-recovered) error was seen, so the
// caller can back off.
func (a *Adapter) handlePollErrors(fetches kgo.Fetches) bool {
	if a.pollErrSince == nil {
		a.pollErrSince = make(map[string]time.Time)
	}
	if a.pollErrLogged == nil {
		a.pollErrLogged = make(map[string]time.Time)
	}
	if a.pollErrLastID == nil {
		a.pollErrLastID = make(map[string]string)
	}
	logEvery := a.opts.pollErrorLogInterval
	if logEvery <= 0 {
		logEvery = defaultPollErrorLogInterval
	}

	// A partition that delivered a record this poll has recovered. Mere absence
	// from this poll's errors has not: kgo may just not have fetched it this
	// round, so absence must not reset the streak or the bail timer never grows.
	recovered := make(map[string]struct{})
	fetches.EachRecord(func(r *kgo.Record) {
		recovered[r.Topic+"-"+strconv.Itoa(int(r.Partition))] = struct{}{}
	})
	for key := range recovered {
		delete(a.pollErrSince, key)
		delete(a.pollErrLogged, key)
		delete(a.pollErrLastID, key)
	}

	now := time.Now()
	var bailReason error
	var sawErr bool

	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		key := topic + "-" + strconv.Itoa(int(partition))
		if _, recoveredThisPoll := recovered[key]; recoveredThisPoll {
			return // delivered a record despite an error; treat as progress
		}
		sawErr = true

		since, ok := a.pollErrSince[key]
		if !ok {
			since = now
			a.pollErrSince[key] = since
		}
		streak := now.Sub(since)

		// Log on the first error, once the window elapses, or when the error
		// changed (so distinct failure modes are not hidden by the throttle).
		id := errIdentity(err)
		last, logged := a.pollErrLogged[key]
		changed := id != a.pollErrLastID[key]
		if !logged || changed || now.Sub(last) >= logEvery {
			a.logger.Error(a.ctx, fmt.Sprintf("poll error on %s[%d] (failing for %s): %v",
				topic, partition, streak.Truncate(time.Second), err))
			a.pollErrLogged[key] = now
			a.pollErrLastID[key] = id
		}

		if a.opts.pollErrorBailAfter > 0 && streak >= a.opts.pollErrorBailAfter {
			bailReason = fmt.Errorf("partition %s[%d] failing to fetch for %s: %w",
				topic, partition, a.opts.pollErrorBailAfter, err)
		}
	})

	if bailReason != nil {
		a.bail(bailReason)
	}
	return sawErr
}

// errIdentity returns a stable fingerprint used to detect when a partition's
// poll error has changed. Kafka protocol errors collapse to their numeric code
// (their string form is constant per code); any other error uses its full
// string, so distinct network/IO failures (which embed addresses) are treated
// as distinct and each gets surfaced.
func errIdentity(err error) string {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return "kafka:" + strconv.Itoa(int(ke.Code))
	}
	return err.Error()
}

// bail stops the consumer once, asynchronously, after sustained poll failures,
// then terminates the process so the orchestrator reschedules the pod rather
// than leaving a zombie replica consuming nothing. The whole sequence runs in a
// goroutine because Shutdown stops this very polling loop; calling it inline
// would deadlock. Termination happens after Shutdown so offsets are committed
// and in-flight work drains first; it runs even if Shutdown errors, because a
// stuck partition must not keep the pod alive. The terminate action is
// configurable via WithBailTerminate (default os.Exit(1)).
func (a *Adapter) bail(reason error) {
	a.bailOnce.Do(func() {
		a.logger.Error(a.ctx, fmt.Sprintf("stopping consumer after sustained poll failure: %v", reason))
		go func() {
			if a.adaptedConsumer != nil {
				if err := a.adaptedConsumer.Shutdown(); err != nil {
					a.logger.Error(a.ctx, fmt.Sprintf("shutdown after poll-error bail failed: %v", err))
				}
			}
			terminate := a.opts.bailTerminate
			if terminate == nil {
				terminate = defaultBailTerminate
			}
			a.logger.Error(a.ctx, "terminating process after poll-error bail so the orchestrator can reschedule")
			terminate()
		}()
	})
}

// ExtractEnvelope maps a kgo.Record to nexus.Envelope.
//
// Key extraction handles all key types safely:
//   - UTF-8 string keys are used directly
//   - Binary keys are base64 encoded (safe, deterministic)
//   - Empty keys fall back to partition number
//
// For optimal performance or custom context injection (traces/spans),
// override via builder.WithExtractEnvelope().
func (a *Adapter) ExtractEnvelope(record *kgo.Record) nexus.Envelope {
	var key string
	msgKey := record.Key

	switch {
	case len(msgKey) > 0 && utf8.Valid(msgKey):
		key = string(msgKey)
	case len(msgKey) > 0:
		key = base64.StdEncoding.EncodeToString(msgKey)
	default:
		key = strconv.Itoa(int(record.Partition))
	}

	return nexus.Envelope{
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       key,
		Ctx:       a.ctx,
	}
}

// CommitOffsets commits the specified messages to the broker.
func (a *Adapter) CommitOffsets(messages []*nexus.Message[*kgo.Record]) ([]*nexus.Message[*kgo.Record], error) {
	if len(messages) == 0 {
		return nil, nil
	}

	records := make([]*kgo.Record, 0, len(messages))
	for _, msg := range messages {
		records = append(records, *msg.Payload)
	}

	if err := a.commitRecordsFn(a.ctx, records...); err != nil {
		a.logger.Error(a.ctx, fmt.Sprintf("failed to commit offsets: %v", err))
		a.logGroupMembershipHintIfApplicable(err)
		return nil, fmt.Errorf("commit records failed: %w", err)
	}

	a.logger.Debug(a.ctx, fmt.Sprintf("committed %d offsets", len(records)))
	return nil, nil
}

// logGroupMembershipHintIfApplicable surfaces an actionable advisory when a commit
// fails because the broker has already removed this consumer from the group
// (typically session.timeout.ms exceeded during drain pressure). The advice is
// terminal - retrying is pointless because group membership is already gone -
// so the operator-facing fix is to give the consumer more heartbeat slack.
func (a *Adapter) logGroupMembershipHintIfApplicable(err error) {
	if !errors.Is(err, kerr.UnknownMemberID) && !errors.Is(err, kerr.IllegalGeneration) {
		return
	}
	a.logger.Warn(a.ctx, fmt.Sprintf(
		"commit rejected (group membership lost): session timeout currently %s - likely exceeded during drain, raise via kgo.SessionTimeout(...)",
		a.currentSessionTimeoutDescription()))
}

// currentSessionTimeoutDescription returns a human-readable description of the
// effective session timeout, using franz-go's runtime introspection. Returns the
// internal default (45s) when the option wasn't overridden by the caller.
func (a *Adapter) currentSessionTimeoutDescription() string {
	if a.client == nil {
		return "<unknown - client not initialised>"
	}
	if d, ok := a.client.OptValue(kgo.SessionTimeout).(time.Duration); ok {
		return d.String()
	}
	return "<unknown>"
}

// AckRebalance acknowledges rebalance completion.
// franz-go handles acks internally, so this is a no-op.
func (a *Adapter) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

// BrokerQuery for committed offsets and other queries.
// This is a no-op for franz-go.
func (a *Adapter) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

// ConsumerGroup returns the consumer group ID configured on this adapter.
// Returns "" for adapters created with NewCustom() where no group was specified.
func (a *Adapter) ConsumerGroup() string {
	return a.groupID
}
