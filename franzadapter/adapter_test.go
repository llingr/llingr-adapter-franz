// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNew(t *testing.T) {
	t.Run("creates adapter with brokers and groupID", func(t *testing.T) {
		ctx := context.Background()
		adapter := New(ctx, "test-group", "broker1:9092", "broker2:9092")

		if adapter == nil {
			t.Fatal("expected adapter, got nil")
		}
		if adapter.groupID != "test-group" {
			t.Errorf("groupID = %q, want %q", adapter.groupID, "test-group")
		}
		if len(adapter.brokers) != 2 {
			t.Errorf("brokers length = %d, want 2", len(adapter.brokers))
		}
		if adapter.brokers[0] != "broker1:9092" {
			t.Errorf("brokers[0] = %q, want %q", adapter.brokers[0], "broker1:9092")
		}
		if adapter.brokers[1] != "broker2:9092" {
			t.Errorf("brokers[1] = %q, want %q", adapter.brokers[1], "broker2:9092")
		}
	})

	t.Run("initialises pending rebalance maps", func(t *testing.T) {
		ctx := context.Background()
		adapter := New(ctx, "test-group", "broker:9092")

		if adapter.pendingAssigned == nil {
			t.Error("pendingAssigned should be initialised")
		}
		if adapter.pendingRevoked == nil {
			t.Error("pendingRevoked should be initialised")
		}
	})

	t.Run("client is nil until CreateConsumer", func(t *testing.T) {
		ctx := context.Background()
		adapter := New(ctx, "test-group", "broker:9092")

		if adapter.client != nil {
			t.Error("client should be nil before CreateConsumer")
		}
	})

	t.Run("stores init context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		adapter := New(ctx, "test-group", "broker:9092")

		if adapter.initCtx != ctx {
			t.Error("initCtx should be stored for later use")
		}
	})
}

func TestNewWithOptions(t *testing.T) {
	t.Run("stores user options", func(t *testing.T) {
		ctx := context.Background()
		opts := []kgo.Opt{
			kgo.SessionTimeout(30 * time.Second),
			kgo.RebalanceTimeout(60 * time.Second),
		}

		adapter := NewWithOptions(ctx, "test-group", []string{"broker:9092"}, opts...)

		if len(adapter.userOpts) != 2 {
			t.Errorf("userOpts length = %d, want 2", len(adapter.userOpts))
		}
	})

	t.Run("handles no options", func(t *testing.T) {
		ctx := context.Background()
		adapter := NewWithOptions(ctx, "test-group", []string{"broker:9092"})

		if len(adapter.userOpts) != 0 {
			t.Errorf("userOpts length = %d, want 0", len(adapter.userOpts))
		}
	})

	t.Run("stores brokers as slice", func(t *testing.T) {
		ctx := context.Background()
		brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}

		adapter := NewWithOptions(ctx, "test-group", brokers)

		if len(adapter.brokers) != 3 {
			t.Errorf("brokers length = %d, want 3", len(adapter.brokers))
		}
	})
}

func TestNewWithOptions_PanicsOnEmptyGroup(t *testing.T) {
	for _, name := range []string{"NewWithOptions", "New"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic on empty consumer group")
				}
				msg, _ := r.(string)
				if !strings.Contains(msg, "consumer group") {
					t.Errorf("panic should name the consumer group, got: %v", r)
				}
			}()
			if name == "New" {
				New(context.Background(), "", "broker:9092")
			} else {
				NewWithOptions(context.Background(), "", []string{"broker:9092"})
			}
		})
	}
}

func TestNewCustom(t *testing.T) {
	t.Run("creates empty adapter", func(t *testing.T) {
		adapter := NewCustom()

		if adapter == nil {
			t.Fatal("expected adapter, got nil")
		}
		if adapter.client != nil {
			t.Error("client should be nil")
		}
		if adapter.groupID != "" {
			t.Errorf("groupID = %q, want empty", adapter.groupID)
		}
		if adapter.brokers != nil {
			t.Errorf("brokers = %v, want nil", adapter.brokers)
		}
	})

	t.Run("initialises pending rebalance maps", func(t *testing.T) {
		adapter := NewCustom()

		if adapter.pendingAssigned == nil {
			t.Error("pendingAssigned should be initialised")
		}
		if adapter.pendingRevoked == nil {
			t.Error("pendingRevoked should be initialised")
		}
	})

	t.Run("seeds default poll-error options", func(t *testing.T) {
		o := NewCustom().opts
		if o.pollErrorBailAfter != 10*time.Minute {
			t.Errorf("PollErrorBailAfter = %s, want 10m", o.pollErrorBailAfter)
		}
		if o.pollErrorBackoff != 25*time.Millisecond {
			t.Errorf("PollErrorBackoff = %s, want 25ms", o.pollErrorBackoff)
		}
		if o.pollErrorLogInterval != time.Second {
			t.Errorf("PollErrorLogInterval = %s, want 1s", o.pollErrorLogInterval)
		}
		if o.bailTerminate == nil {
			t.Error("BailTerminate should default to a non-nil action")
		}
	})
}

// TestWithOptions is a higher-level check that the options reach the adapter:
// the option-resolution logic itself is unit-tested in adapter_options_test.go.
func TestWithOptions(t *testing.T) {
	t.Run("folds options over defaults and validates", func(t *testing.T) {
		a := NewCustom().WithOptions(WithPollErrorBackoff(30 * time.Second))
		if a.opts.pollErrorBackoff != 5*time.Second {
			t.Errorf("backoff = %s, want clamped 5s", a.opts.pollErrorBackoff)
		}
		if a.opts.pollErrorBailAfter != 10*time.Minute {
			t.Errorf("unset bailAfter = %s, want default 10m", a.opts.pollErrorBailAfter)
		}
		if a.opts.bailTerminate == nil {
			t.Error("unset BailTerminate should default to non-nil")
		}
	})
	t.Run("zero disables bail and backoff", func(t *testing.T) {
		a := NewCustom().WithOptions(WithPollErrorBailAfter(0), WithPollErrorBackoff(0))
		if a.opts.pollErrorBailAfter != 0 || a.opts.pollErrorBackoff != 0 {
			t.Errorf("zero should disable: bail=%s backoff=%s",
				a.opts.pollErrorBailAfter, a.opts.pollErrorBackoff)
		}
	})
}

func TestSetClient(t *testing.T) {
	t.Run("sets client on adapter", func(t *testing.T) {
		adapter := NewCustom()

		// create a minimal client (will fail to connect but that's ok for this test)
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		adapter.SetClient(client)

		if adapter.client != client {
			t.Error("client should be set on adapter")
		}
	})
}

func TestRequiredOpts(t *testing.T) {
	opts := NewCustom().RequiredOpts()
	if len(opts) != 5 {
		t.Fatalf("RequiredOpts len = %d, want 5", len(opts))
	}

	// A client built with RequiredOpts must satisfy the enforced client checks.
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("test-topic"),
	}, opts...)...)
	if err != nil {
		t.Fatalf("failed to build client with RequiredOpts: %v", err)
	}
	defer client.Close()

	if v, _ := client.OptValue(kgo.DisableAutoCommit).(bool); !v {
		t.Error("RequiredOpts should disable auto-commit")
	}
	if v, _ := client.OptValue(kgo.BlockRebalanceOnPoll).(bool); !v {
		t.Error("RequiredOpts should set BlockRebalanceOnPoll")
	}
}

func TestCreateConsumer_Errors(t *testing.T) {
	t.Run("fails without client or config", func(t *testing.T) {
		adapter := NewCustom()

		builder := &mockConsumerBuilder{topicName: "test-topic"}
		_, err := adapter.CreateConsumer(builder)

		if err == nil {
			t.Error("expected error, got nil")
		}
		expectedMsg := "no kgo.Client configured"
		if err != nil && !contains(err.Error(), expectedMsg) {
			t.Errorf("error = %q, want to contain %q", err.Error(), expectedMsg)
		}
	})

	t.Run("stores topic name from builder", func(t *testing.T) {
		adapter := NewCustom()

		builder := &mockConsumerBuilder{topicName: "orders"}
		_, _ = adapter.CreateConsumer(builder) // will fail but that's ok

		if adapter.topicName != "orders" {
			t.Errorf("topicName = %q, want %q", adapter.topicName, "orders")
		}
	})

	// Covers the initClient path: when CreateConsumer is called on an adapter
	// constructed via New()/NewWithOptions (client==nil, groupID set), it must
	// invoke initClient which tries to dial the brokers and Ping. We point at
	// an unreachable port and use a short context so the Ping fails quickly,
	// exercising the "kgo.NewClient + Ping returns error" branch.
	t.Run("propagates initClient connection error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		adapter := New(ctx, "test-group", "127.0.0.1:1") // RFC 5735 + reserved port = no listener
		builder := &mockConsumerBuilder{topicName: "test-topic"}
		_, err := adapter.CreateConsumer(builder)
		if err == nil {
			t.Fatal("expected initClient to fail against unreachable broker")
		}
		// Surface should mention either client creation or broker connection.
		if !contains(err.Error(), "kgo client") && !contains(err.Error(), "connect to broker") {
			t.Errorf("unexpected error wording: %q", err.Error())
		}
	})

	// Covers the bwCollector topicName propagation branch.
	t.Run("propagates topic name into bandwidth collector", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		adapter := New(ctx, "test-group", "127.0.0.1:1")
		adapter.WithBandwidthInterval(0) // installs a collector with default interval

		builder := &mockConsumerBuilder{topicName: "metering-topic"}
		_, _ = adapter.CreateConsumer(builder) // expected to fail at Ping; we only care about the early assignment

		if adapter.bwCollector == nil {
			t.Fatal("bwCollector should be installed by WithBandwidthInterval")
		}
		if adapter.bwCollector.topicName != "metering-topic" {
			t.Errorf("bwCollector.topicName = %q, want %q",
				adapter.bwCollector.topicName, "metering-topic")
		}
	})
}

// mockConsumerBuilder implements nexus.ConsumerBuilder[*kgo.Record] for testing
type mockConsumerBuilder struct {
	topicName   string
	buildCalled bool
}

func (m *mockConsumerBuilder) TopicName() string {
	return m.topicName
}

func (m *mockConsumerBuilder) Build(_ nexus.BrokerPort[*kgo.Record]) nexus.AdaptedConsumer[*kgo.Record] {
	m.buildCalled = true
	return &mockAdaptedConsumer{ctx: context.Background(), logger: &mockLogger{}}
}

// mockAdaptedConsumer implements nexus.AdaptedConsumer[*kgo.Record] for testing
type mockAdaptedConsumer struct {
	triggerRebalanceCalled bool
	lastRebalanceType      nexus.RebalanceType
	lastRebalanceInfo      []nexus.RebalanceInfo
	ctx                    context.Context
	logger                 nexus.Logger
	shutdownCalled         atomic.Bool
}

func (m *mockAdaptedConsumer) Subscribe() error         { return nil }
func (m *mockAdaptedConsumer) Shutdown() error          { m.shutdownCalled.Store(true); return nil }
func (m *mockAdaptedConsumer) TopicName() string        { return "test-topic" }
func (m *mockAdaptedConsumer) Context() context.Context { return m.ctx }
func (m *mockAdaptedConsumer) Logger() nexus.Logger     { return m.logger }
func (m *mockAdaptedConsumer) TriggerRebalance(rebalanceType nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	m.triggerRebalanceCalled = true
	m.lastRebalanceType = rebalanceType
	m.lastRebalanceInfo = info
	return nil
}

// mockLogger implements nexus.Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (m *mockLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Error(_ context.Context, _ string, _ ...any) {}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
