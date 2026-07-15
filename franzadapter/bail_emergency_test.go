// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newBailAdapter wires an adapter for direct bail() testing:
// leaveGroupFn and closeFn are replaced with counters so tests
// can assert the broker release ran exactly once.
func newBailAdapter(t *testing.T) (*Adapter, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	prepareFetchCtx(adapter)
	adapter.fetchLoopDone = make(chan struct{})

	// close latch so that self-release won't burn the fetch-loop
	// stop timeout (the loop is not running in these unit tests)
	// and release proceeds immediately
	close(adapter.fetchLoopDone)

	leaves := &atomic.Int32{}
	closes := &atomic.Int32{}
	adapter.leaveGroupFn = func(_ context.Context) error { leaves.Add(1); return nil }
	adapter.closeFn = func() { closes.Add(1) }
	return adapter, leaves, closes
}

// bail trips the consumer's emergency shutdown with the reason and releases
// the adapter's own client; the graceful Shutdown never runs.
func TestBail_TripsAndSelfReleases(t *testing.T) {
	adapter, leaves, closes := newBailAdapter(t)
	consumer := &mockAdaptedConsumer{ctx: context.Background(), logger: &mockLogger{}}
	adapter.adaptedConsumer = consumer

	adapter.bail(errors.New("sustained poll failure on p1"))

	if got := consumer.trips.Load(); got != 1 {
		t.Fatalf("EmergencyShutdown called %d times, want exactly 1", got)
	}
	if reason := consumer.lastTripReason.Load(); reason == nil ||
		!strings.Contains((*reason).Error(), "sustained poll failure") {
		t.Errorf("trip reason = %v, want the bail reason", reason)
	}

	// the self-release runs on its own goroutine; wait for the close
	deadline := time.Now().Add(2 * time.Second)
	for closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := leaves.Load(); got != 1 {
		t.Errorf("leave group called %d times, want 1", got)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("client close called %d times, want 1", got)
	}

	if consumer.shutdownCalled.Load() {
		t.Error("graceful Shutdown must not run on the bail path")
	}
}

// bailOnce still bounds the whole sequence: repeated bails (the poll-error
// streak re-arms every poll past the threshold) trip the consumer once.
func TestBail_RepeatedBailsTripOnce(t *testing.T) {
	adapter, _, closes := newBailAdapter(t)
	consumer := &mockAdaptedConsumer{ctx: context.Background(), logger: &mockLogger{}}
	adapter.adaptedConsumer = consumer

	for i := 0; i < 5; i++ {
		adapter.bail(errors.New("streak re-armed"))
	}

	if got := consumer.trips.Load(); got != 1 {
		t.Errorf("EmergencyShutdown called %d times across 5 bails, want 1", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("client close called %d times, want 1", got)
	}
}

// Unsubscribe is idempotent: sequential and concurrent callers (the engine's
// drain, bail's self-release, a host-side cleanup) produce exactly one leave
// and one close, and every caller returns only once the release completed.
func TestUnsubscribe_IdempotentAcrossCallers(t *testing.T) {
	adapter, leaves, closes := newBailAdapter(t)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = adapter.Unsubscribe()
		}()
	}
	wg.Wait()
	_ = adapter.Unsubscribe() // late sequential repeat

	if got := leaves.Load(); got != 1 {
		t.Errorf("leave group called %d times, want exactly 1", got)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("client close called %d times, want exactly 1", got)
	}
}

// plainConsumer satisfies nexus.AdaptedConsumer but NOT
// nexus.EmergencyShutdowner; CreateConsumer must refuse it at wiring time.
type plainConsumer struct{}

func (plainConsumer) Subscribe() error         { return nil }
func (plainConsumer) Shutdown() error          { return nil }
func (plainConsumer) Context() context.Context { return context.Background() }
func (plainConsumer) Logger() nexus.Logger     { return &mockLogger{} }
func (plainConsumer) TriggerRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

type plainConsumerBuilder struct{}

func (plainConsumerBuilder) TopicName() string { return "orders" }
func (plainConsumerBuilder) Build(nexus.BrokerPort[*kgo.Record]) nexus.AdaptedConsumer[*kgo.Record] {
	return plainConsumer{}
}

// The EmergencyShutdowner contract is enforced at wiring time: a consumer
// without EmergencyShutdown(reason error) is refused with an actionable
// error, not accepted with a bail that cannot escalate.
func TestCreateConsumer_RejectsConsumerWithoutEmergencyShutdown(t *testing.T) {
	adapter := NewCustom()
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers("localhost:1"),
		kgo.ConsumerGroup("wiring-group"),
		kgo.ConsumeTopics("orders"),
	}, adapter.RequiredOpts()...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer client.Close()
	adapter.SetClient(client)

	_, err = adapter.CreateConsumer(plainConsumerBuilder{})
	if err == nil {
		t.Fatal("want CreateConsumer to reject a consumer without EmergencyShutdown, got nil error")
	}
	if !strings.Contains(err.Error(), "EmergencyShutdown(reason error)") {
		t.Errorf("error should name the required method, got: %v", err)
	}
}
