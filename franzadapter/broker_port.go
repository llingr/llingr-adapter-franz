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
// Uses the BlockRebalanceOnPoll pattern with the gate released at the TOP of
// each Poll, before the fetch: a rebalance may begin only between Poll calls,
// i.e. only after the record returned by the previous Poll has been dispatched
// into the pipeline by the polling loop. The gate is never released while a
// record is in hand but undelivered, so a revoke's drain always sees every
// delivered record.
func (a *Adapter) Poll(timeout time.Duration) (*kgo.Record, bool, error) {
	if a.pollRecordsFn == nil {
		return nil, false, fmt.Errorf("client closed")
	}

	// A record stashed by a previous poll's failed assign trigger takes
	// precedence over fetching. The rebalance gate stays closed: the record is
	// in hand and undelivered (see the gate invariant below).
	if a.pendingRecord != nil {
		record := a.pendingRecord
		a.pendingRecord = nil
		if err := a.processPendingRebalances(); err != nil {
			a.pendingRecord = record
			return nil, false, err
		}
		return record, true, nil
	}

	// Release the rebalance gate BEFORE fetching, not after. By the time this
	// call runs, the record returned by the previous Poll has been fully
	// dispatched into the pipeline (the polling loop dispatches synchronously
	// before polling again), so a revoke that starts now is drained AFTER that
	// record is visible to the drain. The old order (release after PollRecords,
	// before returning the record) opened the gate while this call's record was
	// still in hand and undelivered: a cooperative revoke could drain and commit
	// without it, the record's work was never committed, and the partition's
	// next owner re-read it (duplicates). franz-go semantics: a poll that
	// returned records keeps its poller registered (blocking rebalances) until
	// AllowRebalance; an empty poll self-releases its poller, so an idle
	// consumer never blocks a rebalance.
	if a.allowRebalanceFn != nil {
		a.allowRebalanceFn()
	}

	ctx, cancel := context.WithTimeout(a.ctx, timeout)
	defer cancel()

	fetches := a.pollRecordsFn(ctx, 1)

	if fetches.IsClientClosed() {
		return nil, false, fmt.Errorf("client closed")
	}

	// Deliver a record from a healthy partition first; a single failing
	// partition must not starve the others.
	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if record == nil {
			record = r
		}
	})

	if err := a.processPendingRebalances(); err != nil {
		// the client already consumed this fetch's record: stash it for the
		// next poll rather than lose it (at-least-once)
		a.pendingRecord = record
		return nil, false, err
	}

	// Single-topic contract: the engine's offset tracking is keyed by partition
	// alone, so a record from another topic (a multi-topic or pattern
	// subscription slipped in via NewCustom) would cross-contaminate committed
	// offsets. Refuse it and stop the consumer: this is fatal misconfiguration.
	if record != nil && a.topicName != "" && record.Topic != a.topicName {
		err := fmt.Errorf("record from topic %q on a consumer configured for %q - "+
			"one consumer serves one topic; check the client's topic subscription",
			record.Topic, a.topicName)
		a.logger.Error(a.ctx, err.Error())
		a.bail(err)
		return nil, false, err
	}

	// Rate-limit-log per-partition fetch errors and bail if one persists.
	// Errors are absorbed (not returned) so the consumer's poll loop does not
	// re-log them on every poll.
	sawErr := a.handlePollErrors(fetches)

	if record != nil {
		// DIAGNOSTIC TRACE COMMENTED OUT (not removed) for the mutex-vs-coincidence
		// experiment. This call takes a.mu on the per-record delivery path; that added
		// synchronization (with the arming in triggerAssign) is the suspected reason the
		// residual franz handoff duplicate stopped reproducing. We re-run the yoyo several
		// times WITHOUT it to measure the real reproduction rate. It stays here, commented,
		// so the offset-level handoff logging can be re-enabled to diagnose the residual
		// once we have confirmed it is real rather than a timing artefact of this lock.
		// Note a.mu is still taken by OnAssigned/processPendingRebalances (pre-trace); only
		// the per-record delivery lock is removed, which is exactly the variable under test.
		// a.traceFirstReadAfterAssign(record)
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

// DIAGNOSTIC TRACE commented out for the mutex-vs-coincidence experiment (see the note at
// the call site in Poll). Re-enable together with the field in adapter.go and the arming in
// rebalance.go's triggerAssign.
/*
func (a *Adapter) traceFirstReadAfterAssign(record *kgo.Record) {
	a.mu.Lock()
	awaiting := a.traceAwaitFirstRead[record.Partition]
	if awaiting {
		delete(a.traceAwaitFirstRead, record.Partition)
	}
	a.mu.Unlock()
	if awaiting {
		a.logger.Info(a.ctx, fmt.Sprintf("TRACE first-read after assign: partition=%d offset=%d",
			record.Partition, record.Offset))
	}
}
*/

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

	// DIAGNOSTIC TRACE commented out for the mutex-vs-coincidence experiment (see Poll). This
	// block does not take a.mu (pure logging), but it is part of the same trace and its log
	// volume would muddy a clean baseline run, so it is disabled too.
	/*
		hi := make(map[int32]int64, 4)
		seen := make(map[int32]bool, 4)
		for _, r := range records {
			if !seen[r.Partition] || r.Offset > hi[r.Partition] {
				hi[r.Partition] = r.Offset
				seen[r.Partition] = true
			}
		}
		a.logger.Info(a.ctx, fmt.Sprintf("TRACE commit (highest committed record offset per partition) %v", hi))
	*/

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
