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

	// check for errors (ignore timeout)
	var pollErr error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		a.logger.Error(a.ctx, fmt.Sprintf("poll error on %s[%d]: %v", topic, partition, err))
		pollErr = err
	})
	if pollErr != nil {
		return nil, false, pollErr
	}

	var record *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if record == nil {
			record = r
		}
	})

	if record == nil {
		return nil, false, nil
	}

	return record, true, nil
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
