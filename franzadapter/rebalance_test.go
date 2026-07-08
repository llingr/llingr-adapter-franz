// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/llingr/llingr-nexus/nexus"
)

const testOrdersTopic = "orders"

func TestOnAssigned(t *testing.T) {
	t.Run("queues partitions to pendingAssigned", func(t *testing.T) {
		adapter := NewCustom()

		assigned := map[string][]int32{
			testOrdersTopic: {0, 1, 2},
		}

		adapter.OnAssigned(context.Background(), nil, assigned)

		if len(adapter.pendingAssigned) != 1 {
			t.Errorf("pendingAssigned topics = %d, want 1", len(adapter.pendingAssigned))
		}
		if len(adapter.pendingAssigned[testOrdersTopic]) != 3 {
			t.Errorf("pendingAssigned[orders] = %d partitions, want 3",
				len(adapter.pendingAssigned[testOrdersTopic]))
		}
	})

	t.Run("appends multiple calls", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnAssigned(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
		})
		adapter.OnAssigned(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {2, 3},
		})

		if len(adapter.pendingAssigned[testOrdersTopic]) != 4 {
			t.Errorf("pendingAssigned[orders] = %d partitions, want 4",
				len(adapter.pendingAssigned[testOrdersTopic]))
		}
	})

	t.Run("handles multiple topics", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnAssigned(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
			"payments":      {0},
		})

		if len(adapter.pendingAssigned) != 2 {
			t.Errorf("pendingAssigned topics = %d, want 2", len(adapter.pendingAssigned))
		}
		if len(adapter.pendingAssigned[testOrdersTopic]) != 2 {
			t.Error("orders should have 2 partitions")
		}
		if len(adapter.pendingAssigned["payments"]) != 1 {
			t.Error("payments should have 1 partition")
		}
	})

	t.Run("does not affect pendingRevoked", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnAssigned(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1, 2},
		})

		if len(adapter.pendingRevoked) != 0 {
			t.Error("OnAssigned should not affect pendingRevoked")
		}
	})
}

func TestOnRevoked(t *testing.T) {
	t.Run("drains and commits synchronously (triggers Revoke, does not queue)", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
		})

		// Revoke must be triggered inline: franz-go holds reassignment until this
		// callback returns, so the drain+commit has to happen here, not deferred.
		if !consumer.triggerRebalanceCalled {
			t.Fatal("OnRevoked should trigger a rebalance synchronously")
		}
		if consumer.lastRebalanceType != nexus.Revoke {
			t.Errorf("rebalance type = %v, want Revoke", consumer.lastRebalanceType)
		}
		if len(consumer.lastRebalanceInfo) != 2 {
			t.Fatalf("RebalanceInfo count = %d, want 2", len(consumer.lastRebalanceInfo))
		}
		// Must NOT defer: the deferred path is what let franz-go reassign before the
		// commit landed, causing duplicate reprocessing under rebalance churn.
		if len(adapter.pendingRevoked) != 0 {
			t.Error("OnRevoked must not queue revokes to pendingRevoked")
		}
	})

	t.Run("each call triggers immediately", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &orderTrackingConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{testOrdersTopic: {0}})
		adapter.OnRevoked(context.Background(), nil, map[string][]int32{testOrdersTopic: {1, 2}})

		if len(consumer.callOrder) != 2 {
			t.Fatalf("expected 2 Revoke triggers, got %d", len(consumer.callOrder))
		}
		for i, rt := range consumer.callOrder {
			if rt != nexus.Revoke {
				t.Errorf("call %d = %v, want Revoke", i, rt)
			}
		}
	})

	t.Run("builds correct RebalanceInfo", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{"payments": {1, 2}})

		if consumer.lastRebalanceType != nexus.Revoke {
			t.Errorf("rebalance type = %v, want Revoke", consumer.lastRebalanceType)
		}
		if len(consumer.lastRebalanceInfo) != 2 {
			t.Fatalf("RebalanceInfo count = %d, want 2", len(consumer.lastRebalanceInfo))
		}
		for _, info := range consumer.lastRebalanceInfo {
			if info.TopicName != "payments" {
				t.Errorf("TopicName = %q, want payments", info.TopicName)
			}
			if info.RebalanceType != nexus.Revoke {
				t.Errorf("RebalanceType = %v, want Revoke", info.RebalanceType)
			}
		}
	})

	t.Run("does not affect pendingAssigned", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
		})

		if len(adapter.pendingAssigned) != 0 {
			t.Error("OnRevoked should not affect pendingAssigned")
		}
	})

	t.Run("handles nil adaptedConsumer safely", func(t *testing.T) {
		adapter := NewCustom()
		adapter.adaptedConsumer = nil // not yet wired
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		// must not panic
		adapter.OnRevoked(context.Background(), nil, map[string][]int32{testOrdersTopic: {0}})
	})
}

func TestOnLost(t *testing.T) {
	t.Run("delegates to OnRevoked (triggers Revoke synchronously)", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnLost(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1, 2},
		})

		if !consumer.triggerRebalanceCalled {
			t.Fatal("OnLost should trigger a Revoke rebalance via OnRevoked")
		}
		if consumer.lastRebalanceType != nexus.Revoke {
			t.Errorf("rebalance type = %v, want Revoke", consumer.lastRebalanceType)
		}
		if len(consumer.lastRebalanceInfo) != 3 {
			t.Errorf("RebalanceInfo count = %d, want 3", len(consumer.lastRebalanceInfo))
		}
	})

	t.Run("pendingAssigned remains empty", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.OnLost(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0},
		})

		if len(adapter.pendingAssigned) != 0 {
			t.Error("OnLost should not affect pendingAssigned")
		}
	})

	t.Run("tolerates a failing revoke trigger", func(t *testing.T) {
		// Lost partitions are already gone (fencing, session timeout): the
		// engine's drain/commit may fail. The callback must log and return
		// normally - a panic here would take down the client's poll goroutine.
		adapter := NewCustom()
		adapter.adaptedConsumer = &failingRevokeConsumer{}
		logger := &capturingLogger{}
		adapter.logger = logger
		adapter.ctx = context.Background()

		adapter.OnLost(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
		})

		if !logger.errorCalled {
			t.Error("expected the failed revoke trigger to be logged as an error")
		}
	})
}

// failingRevokeConsumer rejects every revoke trigger.
type failingRevokeConsumer struct {
	mockAdaptedConsumer
}

func (c *failingRevokeConsumer) TriggerRebalance(rebalanceType nexus.RebalanceType,
	info []nexus.RebalanceInfo) error {
	if rebalanceType == nexus.Revoke {
		return fmt.Errorf("injected revoke failure")
	}
	return c.mockAdaptedConsumer.TriggerRebalance(rebalanceType, info)
}

// TestRebalanceInfo_UnknownBaselineIsMinusOne: this adapter cannot supply the
// broker's committed offset at assign time (franz-go resolves fetch positions
// internally; querying would cost a broker RPC in the rebalance path), so
// RebalanceInfo.CommittedOffset carries an "unknown" sentinel.
//
// The sentinel must be -1, never 0: offset 0 is a valid (if rare) Kafka
// offset - the first record of a young partition - so 0 is indistinguishable
// from a real committed position, while -1 can never be a real offset and is
// unambiguous. Leaving the field at its zero value silently reported
// "committed offset 0" instead of "unknown".
func TestRebalanceInfo_UnknownBaselineIsMinusOne(t *testing.T) {
	t.Run("assign carries -1", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		if err := adapter.triggerAssign(map[string][]int32{testOrdersTopic: {0, 1}}); err != nil {
			t.Fatalf("triggerAssign: %v", err)
		}

		if len(consumer.lastRebalanceInfo) != 2 {
			t.Fatalf("expected 2 RebalanceInfo entries, got %d", len(consumer.lastRebalanceInfo))
		}
		for _, info := range consumer.lastRebalanceInfo {
			if info.CommittedOffset != -1 {
				t.Errorf("partition %d: CommittedOffset = %d, want -1 (0 is an achievable "+
					"offset and fails open in the engine's baseline guards)",
					info.Partition, info.CommittedOffset)
			}
		}
	})

	t.Run("revoke carries -1", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		if err := adapter.triggerRevoke(map[string][]int32{testOrdersTopic: {2}}); err != nil {
			t.Fatalf("triggerRevoke: %v", err)
		}

		if len(consumer.lastRebalanceInfo) != 1 {
			t.Fatalf("expected 1 RebalanceInfo entry, got %d", len(consumer.lastRebalanceInfo))
		}
		// the engine ignores CommittedOffset on revoke today, but the contract
		// should never expose a fake achievable offset to a future reader
		if got := consumer.lastRebalanceInfo[0].CommittedOffset; got != -1 {
			t.Errorf("CommittedOffset = %d, want -1", got)
		}
	})
}

// TestTriggerAssign_AssignmentOffsetLookup: on assignment the adapter spends
// one batched coordinator round trip to look up the group's committed offsets
// and reports them in RebalanceInfo.CommittedOffset, so consumers start from a
// real baseline. -1 remains the value for "unknown": lookup disabled, lookup
// failed, or a partition with no committed offset.
func TestTriggerAssign_AssignmentOffsetLookup(t *testing.T) {
	newLookupAdapter := func() (*Adapter, *mockAdaptedConsumer, *capturingLogger) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		logger := &capturingLogger{}
		adapter.adaptedConsumer = consumer
		adapter.logger = logger
		adapter.ctx = context.Background()
		return adapter, consumer, logger
	}

	committedOf := func(infos []nexus.RebalanceInfo) map[int32]int64 {
		out := make(map[int32]int64, len(infos))
		for _, info := range infos {
			out[info.Partition] = info.CommittedOffset
		}
		return out
	}

	t.Run("populates fetched committed offsets", func(t *testing.T) {
		adapter, consumer, _ := newLookupAdapter()
		var gotTopics []string
		adapter.fetchCommittedFn = func(_ context.Context, topics []string) (map[string]map[int32]int64, error) {
			gotTopics = topics
			return map[string]map[int32]int64{
				testOrdersTopic: {0: 100, 1: -1}, // partition 2 absent from the response
			}, nil
		}

		if err := adapter.triggerAssign(map[string][]int32{testOrdersTopic: {0, 1, 2}}); err != nil {
			t.Fatalf("triggerAssign: %v", err)
		}

		if len(gotTopics) != 1 || gotTopics[0] != testOrdersTopic {
			t.Errorf("lookup topics = %v, want [%s]", gotTopics, testOrdersTopic)
		}
		got := committedOf(consumer.lastRebalanceInfo)
		want := map[int32]int64{0: 100, 1: -1, 2: -1}
		for partition, offset := range want {
			if got[partition] != offset {
				t.Errorf("partition %d: CommittedOffset = %d, want %d", partition, got[partition], offset)
			}
		}
	})

	t.Run("falls back to -1 and warns on lookup failure", func(t *testing.T) {
		adapter, consumer, logger := newLookupAdapter()
		adapter.fetchCommittedFn = func(_ context.Context, _ []string) (map[string]map[int32]int64, error) {
			return nil, errors.New("coordinator moving")
		}

		if err := adapter.triggerAssign(map[string][]int32{testOrdersTopic: {0}}); err != nil {
			t.Fatalf("triggerAssign must not fail on lookup errors: %v", err)
		}

		if got := committedOf(consumer.lastRebalanceInfo); got[0] != -1 {
			t.Errorf("CommittedOffset = %d, want -1 (unknown) after lookup failure", got[0])
		}
		if !logger.warnCalled {
			t.Error("a failed lookup must be surfaced with a warning")
		}
	})

	t.Run("disabled via WithAssignmentOffsetLookup(false)", func(t *testing.T) {
		adapter, consumer, _ := newLookupAdapter()
		adapter.opts = processAdapterOptions(WithAssignmentOffsetLookup(false))
		lookupCalled := false
		adapter.fetchCommittedFn = func(_ context.Context, _ []string) (map[string]map[int32]int64, error) {
			lookupCalled = true
			return map[string]map[int32]int64{testOrdersTopic: {0: 100}}, nil
		}

		if err := adapter.triggerAssign(map[string][]int32{testOrdersTopic: {0}}); err != nil {
			t.Fatalf("triggerAssign: %v", err)
		}

		if lookupCalled {
			t.Error("lookup must not run when disabled")
		}
		if got := committedOf(consumer.lastRebalanceInfo); got[0] != -1 {
			t.Errorf("CommittedOffset = %d, want -1 (unknown) with lookup disabled", got[0])
		}
	})
}

func TestProcessPendingRebalances(t *testing.T) {
	t.Run("processes queued assigns", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &orderTrackingConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingAssigned[testOrdersTopic] = []int32{0, 1}

		err := adapter.processPendingRebalances()

		if err != nil {
			t.Errorf("processPendingRebalances returned error: %v", err)
		}
		if len(consumer.callOrder) != 1 {
			t.Fatalf("expected 1 TriggerRebalance call, got %d", len(consumer.callOrder))
		}
		if consumer.callOrder[0] != nexus.Assign {
			t.Error("should process assign")
		}
	})

	t.Run("does not process revokes (handled synchronously in OnRevoked)", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &orderTrackingConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		// Even if something were left in pendingRevoked, the poll loop must ignore it:
		// revokes are drained+committed inline in OnRevoked to beat franz-go's reassign.
		adapter.pendingRevoked[testOrdersTopic] = []int32{2, 3}

		_ = adapter.processPendingRebalances()

		for _, rt := range consumer.callOrder {
			if rt == nexus.Revoke {
				t.Error("processPendingRebalances must not trigger revokes")
			}
		}
	})

	t.Run("clears pendingAssigned after processing", func(t *testing.T) {
		adapter := NewCustom()
		adapter.adaptedConsumer = &mockAdaptedConsumer{}
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingAssigned[testOrdersTopic] = []int32{0, 1}

		_ = adapter.processPendingRebalances()

		if len(adapter.pendingAssigned) != 0 {
			t.Error("pendingAssigned should be cleared after processing")
		}
	})

	t.Run("handles nil adaptedConsumer safely", func(t *testing.T) {
		adapter := NewCustom()
		adapter.adaptedConsumer = nil // not yet wired

		adapter.pendingAssigned[testOrdersTopic] = []int32{0}

		err := adapter.processPendingRebalances()

		if err != nil {
			t.Errorf("should handle nil adaptedConsumer gracefully: %v", err)
		}
	})

	t.Run("propagates TriggerRebalance errors from assign", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &erroringConsumer{errOnAssign: true}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingAssigned[testOrdersTopic] = []int32{0}

		err := adapter.processPendingRebalances()

		if err == nil {
			t.Error("should propagate assign error")
		}
		if !contains(err.Error(), "assign") {
			t.Errorf("error should mention assign: %v", err)
		}
	})

	t.Run("builds correct RebalanceInfo for assigns", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingAssigned[testOrdersTopic] = []int32{0, 5, 10}

		_ = adapter.processPendingRebalances()

		if !consumer.triggerRebalanceCalled {
			t.Fatal("TriggerRebalance not called")
		}
		if consumer.lastRebalanceType != nexus.Assign {
			t.Errorf("rebalance type = %v, want Assign", consumer.lastRebalanceType)
		}
		if len(consumer.lastRebalanceInfo) != 3 {
			t.Fatalf("RebalanceInfo count = %d, want 3", len(consumer.lastRebalanceInfo))
		}

		// verify each entry
		for _, info := range consumer.lastRebalanceInfo {
			if info.TopicName != testOrdersTopic {
				t.Errorf("TopicName = %q, want orders", info.TopicName)
			}
			if info.RebalanceType != nexus.Assign {
				t.Errorf("RebalanceType = %v, want Assign", info.RebalanceType)
			}
		}
	})

	t.Run("handles empty pending maps", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer

		err := adapter.processPendingRebalances()

		if err != nil {
			t.Errorf("should handle empty maps: %v", err)
		}
		if consumer.triggerRebalanceCalled {
			t.Error("should not call TriggerRebalance when nothing pending")
		}
	})
}

// orderTrackingConsumer tracks the order of TriggerRebalance calls
type orderTrackingConsumer struct {
	callOrder []nexus.RebalanceType
}

func (c *orderTrackingConsumer) Subscribe() error         { return nil }
func (c *orderTrackingConsumer) Shutdown() error          { return nil }
func (c *orderTrackingConsumer) Context() context.Context { return context.Background() }
func (c *orderTrackingConsumer) Logger() nexus.Logger     { return &mockLogger{} }

func (c *orderTrackingConsumer) TriggerRebalance(rt nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	c.callOrder = append(c.callOrder, rt)
	return nil
}

// erroringConsumer returns errors from TriggerRebalance
type erroringConsumer struct {
	errOnAssign bool
	errOnRevoke bool
}

func (c *erroringConsumer) Subscribe() error         { return nil }
func (c *erroringConsumer) Shutdown() error          { return nil }
func (c *erroringConsumer) Context() context.Context { return context.Background() }
func (c *erroringConsumer) Logger() nexus.Logger     { return &mockLogger{} }

func (c *erroringConsumer) TriggerRebalance(rt nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	if rt == nexus.Assign && c.errOnAssign {
		return errors.New("assign error")
	}
	if rt == nexus.Revoke && c.errOnRevoke {
		return errors.New("revoke error")
	}
	return nil
}
