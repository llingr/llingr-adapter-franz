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

// Subscribe starts the dedicated fetch goroutine. Topic subscription itself is
// done at client creation via kgo.ConsumeTopics(); the group is joined when
// the fetch loop issues its first poll.
func (a *Adapter) Subscribe() error {
	if a.pollRecordsFn != nil {
		a.fetchStart.Do(a.startFetchLoop)
	}
	a.logger.Info(a.ctx, fmt.Sprintf("franz-go subscribed to topic: %s", a.topicName))
	return nil
}

// startFetchLoop wires the fetch-loop plumbing and launches the goroutine.
// Called once, from Subscribe.
func (a *Adapter) startFetchLoop() {
	base := a.ctx
	if base == nil {
		base = context.Background()
	}
	a.fetchCtx, a.fetchCancel = context.WithCancel(base)
	a.fetchedRecords = make(chan *kgo.Record)
	a.recordDispatched = make(chan struct{})
	a.fetchLoopDone = make(chan struct{})
	go a.runFetchLoop()
}

// Unsubscribe stops the fetch loop, then closes the client. Stopping first
// means the loop's deferred AllowRebalance has normally run before the close
// triggers the final leave-group rebalance; if the loop is parked behind an
// in-flight rebalance inside the client past the bounded wait, the close
// proceeds anyway (see fetchLoopStopTimeout).
func (a *Adapter) Unsubscribe() error {
	if a.bwCollector != nil {
		a.bwCollector.stop()
	}
	if a.fetchCancel != nil {
		a.fetchCancel()
		select {
		case <-a.fetchLoopDone:
			if a.logger != nil {
				a.logger.Info(a.ctx, "unsubscribe: fetch loop stopped")
			}
		case <-time.After(fetchLoopStopTimeout):
			a.logger.Warn(a.ctx, fmt.Sprintf(
				"fetch loop did not stop within %s (parked behind an in-flight rebalance); closing anyway",
				fetchLoopStopTimeout))
		}
	}
	// Leave the group explicitly, with the error SURFACED: the client's own
	// close-path leave swallows a failed LeaveGroup request, and the first
	// visible symptom of that is the coordinator expiring this member a full
	// session timeout later, its partitions frozen until then. The wait is
	// bounded for visibility only; a leave still in flight completes in the
	// background and the close below waits for it regardless. Deliberately
	// NOT on a.ctx, which may already be cancelled during shutdown. Ordering:
	// after the fetch-loop stop, because the leave needs the rebalance gate
	// (and if the loop failed to stop, the close's force-open still unblocks
	// it, bounded by leaveGroupTimeout here).
	if a.leaveGroupFn != nil {
		leaveCtx, cancel := context.WithTimeout(context.Background(), leaveGroupTimeout)
		started := time.Now()
		err := a.leaveGroupFn(leaveCtx)
		cancel()
		switch {
		case err == nil:
			if a.logger != nil {
				a.logger.Info(a.ctx, fmt.Sprintf("unsubscribe: left consumer group in %s",
					time.Since(started).Round(time.Millisecond)))
			}
		case errors.Is(err, context.DeadlineExceeded):
			a.logger.Warn(a.ctx, fmt.Sprintf(
				"unsubscribe: leave group still in flight after %s (close will wait for it)", leaveGroupTimeout))
		default:
			a.logger.Warn(a.ctx, fmt.Sprintf(
				"unsubscribe: leave group FAILED: %v - the broker will only evict this member at "+
					"session timeout, and its partitions stay unassigned until then", err))
		}
	}
	if a.closeFn != nil {
		// Close leaves the consumer group and can block behind an in-flight
		// rebalance; bracket it so a shutdown wedged here names this step.
		started := time.Now()
		if a.logger != nil {
			a.logger.Info(a.ctx, "unsubscribe: closing client")
		}
		a.closeFn()
		if a.logger != nil {
			a.logger.Info(a.ctx, fmt.Sprintf("unsubscribe: client closed in %s",
				time.Since(started).Round(time.Millisecond)))
		}
	}
	return nil
}

// Poll returns the next record delivered by the fetch loop, waiting at most
// timeout. It never touches the client, so it can never park inside franz-go's
// untimed poll/rebalance exclusion wait: the polling loop stays responsive to
// the engine's stop and pause signals for the whole duration of any rebalance,
// matching the confluent adapter, whose rebalance callbacks run inline on the
// polling thread.
//
// Gate protocol, engine side: returning a record sets recordAwaitingDispatch;
// the NEXT Poll call is proof that the polling loop has fully dispatched that
// record into the pipeline (it dispatches synchronously before polling again),
// so it signals recordDispatched, and only then does the fetch loop release
// franz-go's rebalance gate. A rebalance can therefore only begin between
// records, so a revoke's drain always sees every record Poll has returned.
func (a *Adapter) Poll(timeout time.Duration) (*kgo.Record, bool, error) {
	if a.pollRecordsFn == nil {
		return nil, false, fmt.Errorf("client closed")
	}

	if a.recordAwaitingDispatch {
		a.recordAwaitingDispatch = false
		select {
		case a.recordDispatched <- struct{}{}:
		case <-a.fetchLoopDone:
		}
	}

	// A record stashed by a previous poll's failed assign trigger takes
	// precedence over the channel. The rebalance gate is still closed: the
	// fetch loop releases it only on the dispatch signal, which is not sent
	// until the stash has been returned and dispatched.
	if a.pendingRecord != nil {
		record := a.pendingRecord
		a.pendingRecord = nil
		if err := a.processPendingRebalances(); err != nil {
			a.pendingRecord = record
			return nil, false, err
		}
		a.recordAwaitingDispatch = true
		return record, true, nil
	}

	var record *kgo.Record
	var loopExited bool
	// Fast path first: skip the timer when a record is already waiting.
	select {
	case r, ok := <-a.fetchedRecords:
		record, loopExited = r, !ok
	default:
		select {
		case r, ok := <-a.fetchedRecords:
			record, loopExited = r, !ok
		case <-time.After(timeout):
		}
	}
	if loopExited {
		return nil, false, fmt.Errorf("client closed")
	}

	if err := a.processPendingRebalances(); err != nil {
		// the fetch loop already consumed this record from the client: stash it
		// for the next poll rather than lose it (at-least-once)
		a.pendingRecord = record
		return nil, false, err
	}

	if record != nil {
		a.recordAwaitingDispatch = true
		return record, true, nil
	}
	return nil, false, nil
}

// runFetchLoop is the dedicated fetch goroutine: the only caller of
// PollRecords and AllowRebalance, and the only goroutine permitted to park
// inside the client (which a poll does, untimed, whenever a rebalance is in
// progress - harmless here because nothing waits on this goroutine's return).
// See the gate protocol on the Adapter fields for why AllowRebalance runs only
// after the dispatch signal.
func (a *Adapter) runFetchLoop() {
	defer close(a.fetchLoopDone)
	defer close(a.fetchedRecords)
	// Whatever state the loop exits in, open the rebalance gate: a poller
	// registration may be held by a delivered-but-undispatched record, by a
	// record dropped on the way out, or by the fake fetch a cancelled or
	// closed poll returns (franz-go registers a poller even for those).
	defer a.allowRebalanceFn()

	for {
		record, exit := a.fetchNext()
		switch {
		case exit:
			return
		case record == nil:
			// Empty poll: franz-go self-released its poller registration, so
			// no gate is held and there is nothing to deliver.
			continue
		}

		select {
		case a.fetchedRecords <- record:
		case <-a.fetchCtx.Done():
			// Stopping with a record in hand: drop it. It was never delivered,
			// so its offset was never committed; the partition's next owner
			// re-reads it (at-least-once).
			return
		}

		select {
		case <-a.recordDispatched:
		case <-a.fetchCtx.Done():
			return
		}

		// The engine called Poll again, so the record above is fully
		// dispatched into the pipeline: a revoke that begins now sees it.
		a.allowRebalanceFn()
	}
}

// fetchNext performs one poll against the client. The context carries no
// per-call timeout: the call parks until data, a rebalance, cancellation, or
// close, and only the fetch goroutine may park there. Returns the fetched
// record (nil when the poll produced none) and whether the loop must exit.
func (a *Adapter) fetchNext() (*kgo.Record, bool) {
	fetches := a.pollRecordsFn(a.fetchCtx, 1)

	if fetches.IsClientClosed() || a.fetchCtx.Err() != nil {
		return nil, true
	}

	// Deliver a record from a healthy partition first; a single failing
	// partition must not starve the others.
	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if record == nil {
			record = r
		}
	})

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
		return nil, true // never deliver the foreign record; the bail shuts the consumer down
	}

	// Rate-limit-log per-partition fetch errors and bail if one persists.
	// Errors are absorbed (not delivered) so they are not re-logged every poll.
	sawErr := a.handlePollErrors(fetches)

	// Broker error with no record: back off briefly so the loop does not spin
	// on a broker that returns buffered errors immediately. Context-aware, and
	// skipped when the backoff is disabled (0).
	if record == nil && sawErr && a.opts.pollErrorBackoff > 0 {
		select {
		case <-time.After(a.opts.pollErrorBackoff):
		case <-a.fetchCtx.Done():
		}
	}

	// DIAGNOSTIC TRACE COMMENTED OUT (not removed) for the mutex-vs-coincidence
	// experiment. This call takes a.mu on the per-record delivery path; that added
	// synchronization (with the arming in triggerAssign) is the suspected reason the
	// residual franz handoff duplicate stopped reproducing. We re-run the yoyo several
	// times WITHOUT it to measure the real reproduction rate. It stays here, commented,
	// so the offset-level handoff logging can be re-enabled to diagnose the residual
	// once we have confirmed it is real rather than a timing artefact of this lock.
	// Note a.mu is still taken by OnAssigned/processPendingRebalances (pre-trace); only
	// the per-record delivery lock is removed, which is exactly the variable under test.
	// if record != nil {
	// 	a.traceFirstReadAfterAssign(record)
	// }
	return record, false
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
// the call site in fetchNext). Re-enable together with the field in adapter.go and the
// arming in rebalance.go's triggerAssign.
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
