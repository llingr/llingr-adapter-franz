// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"errors"
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
	t.Run("queues partitions to pendingRevoked", func(t *testing.T) {
		adapter := NewCustom()

		revoked := map[string][]int32{
			testOrdersTopic: {0, 1},
		}

		adapter.OnRevoked(context.Background(), nil, revoked)

		if len(adapter.pendingRevoked) != 1 {
			t.Errorf("pendingRevoked topics = %d, want 1", len(adapter.pendingRevoked))
		}
		if len(adapter.pendingRevoked[testOrdersTopic]) != 2 {
			t.Errorf("pendingRevoked[orders] = %d partitions, want 2",
				len(adapter.pendingRevoked[testOrdersTopic]))
		}
	})

	t.Run("appends multiple calls", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0},
		})
		adapter.OnRevoked(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {1, 2},
		})

		if len(adapter.pendingRevoked[testOrdersTopic]) != 3 {
			t.Errorf("pendingRevoked[orders] = %d partitions, want 3",
				len(adapter.pendingRevoked[testOrdersTopic]))
		}
	})

	t.Run("does not affect pendingAssigned", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnRevoked(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0, 1},
		})

		if len(adapter.pendingAssigned) != 0 {
			t.Error("OnRevoked should not affect pendingAssigned")
		}
	})
}

func TestOnLost(t *testing.T) {
	t.Run("delegates to OnRevoked", func(t *testing.T) {
		adapter := NewCustom()

		lost := map[string][]int32{
			testOrdersTopic: {0, 1, 2},
		}

		adapter.OnLost(context.Background(), nil, lost)

		// lost partitions should appear in pendingRevoked
		if len(adapter.pendingRevoked) != 1 {
			t.Errorf("pendingRevoked topics = %d, want 1", len(adapter.pendingRevoked))
		}
		if len(adapter.pendingRevoked[testOrdersTopic]) != 3 {
			t.Errorf("pendingRevoked[orders] = %d partitions, want 3",
				len(adapter.pendingRevoked[testOrdersTopic]))
		}
	})

	t.Run("pendingAssigned remains empty", func(t *testing.T) {
		adapter := NewCustom()

		adapter.OnLost(context.Background(), nil, map[string][]int32{
			testOrdersTopic: {0},
		})

		if len(adapter.pendingAssigned) != 0 {
			t.Error("OnLost should not affect pendingAssigned")
		}
	})
}

func TestProcessPendingRebalances(t *testing.T) {
	t.Run("processes revokes before assigns", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &orderTrackingConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		// queue both assign and revoke
		adapter.pendingAssigned[testOrdersTopic] = []int32{0, 1}
		adapter.pendingRevoked[testOrdersTopic] = []int32{2, 3}

		err := adapter.processPendingRebalances()

		if err != nil {
			t.Errorf("processPendingRebalances returned error: %v", err)
		}
		if len(consumer.callOrder) != 2 {
			t.Fatalf("expected 2 TriggerRebalance calls, got %d", len(consumer.callOrder))
		}
		// revoke should come first
		if consumer.callOrder[0] != nexus.Revoke {
			t.Error("revoke should be processed before assign")
		}
		if consumer.callOrder[1] != nexus.Assign {
			t.Error("assign should be processed after revoke")
		}
	})

	t.Run("clears pending maps after processing", func(t *testing.T) {
		adapter := NewCustom()
		adapter.adaptedConsumer = &mockAdaptedConsumer{}
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingAssigned[testOrdersTopic] = []int32{0, 1}
		adapter.pendingRevoked[testOrdersTopic] = []int32{2}

		_ = adapter.processPendingRebalances()

		if len(adapter.pendingAssigned) != 0 {
			t.Error("pendingAssigned should be cleared after processing")
		}
		if len(adapter.pendingRevoked) != 0 {
			t.Error("pendingRevoked should be cleared after processing")
		}
	})

	t.Run("handles nil adaptedConsumer safely", func(t *testing.T) {
		adapter := NewCustom()
		adapter.adaptedConsumer = nil // not yet wired

		adapter.pendingAssigned[testOrdersTopic] = []int32{0}
		adapter.pendingRevoked[testOrdersTopic] = []int32{1}

		err := adapter.processPendingRebalances()

		if err != nil {
			t.Errorf("should handle nil adaptedConsumer gracefully: %v", err)
		}
	})

	t.Run("propagates TriggerRebalance errors from revoke", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &erroringConsumer{errOnRevoke: true}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingRevoked[testOrdersTopic] = []int32{0}

		err := adapter.processPendingRebalances()

		if err == nil {
			t.Error("should propagate revoke error")
		}
		if !contains(err.Error(), "revoke") {
			t.Errorf("error should mention revoke: %v", err)
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

	t.Run("builds correct RebalanceInfo for revokes", func(t *testing.T) {
		adapter := NewCustom()
		consumer := &mockAdaptedConsumer{}
		adapter.adaptedConsumer = consumer
		adapter.logger = &mockLogger{}
		adapter.ctx = context.Background()

		adapter.pendingRevoked["payments"] = []int32{1, 2}

		_ = adapter.processPendingRebalances()

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
