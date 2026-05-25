// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- SimpleConsumerBuilder Tests ---

func TestNewSimpleConsumerBuilder(t *testing.T) {
	t.Run("creates builder with topic and process function", func(t *testing.T) {
		process := func(_ context.Context, _ *nexus.Message[*kgo.Record]) error {
			return nil
		}

		builder := NewSimpleConsumerBuilder(testTopicName, process)

		if builder.topicName != testTopicName {
			t.Errorf("topicName = %q, want %q", builder.topicName, testTopicName)
		}
		if builder.process == nil {
			t.Error("process function should be set")
		}
		if builder.logger == nil {
			t.Error("default logger should be set")
		}
	})

	t.Run("TopicName returns configured topic", func(t *testing.T) {
		builder := NewSimpleConsumerBuilder("orders", nil)

		if builder.TopicName() != "orders" {
			t.Errorf("TopicName() = %q, want %q", builder.TopicName(), "orders")
		}
	})
}

func TestSimpleConsumerBuilder_WithLogger(t *testing.T) {
	customLogger := &mockLogger{}
	builder := NewSimpleConsumerBuilder("test", nil).WithLogger(customLogger)

	if builder.logger != customLogger {
		t.Error("WithLogger should set custom logger")
	}
}

func TestSimpleConsumerBuilder_WithContext(t *testing.T) {
	type ctxKey struct{}
	customCtx := context.WithValue(context.Background(), ctxKey{}, "test-value")
	builder := NewSimpleConsumerBuilder("test", nil).WithContext(customCtx)

	if builder.ctx != customCtx {
		t.Error("WithContext should set custom context")
	}
}

func TestSimpleConsumerBuilder_Build(t *testing.T) {
	t.Run("creates consumer with broker wired", func(t *testing.T) {
		process := func(_ context.Context, _ *nexus.Message[*kgo.Record]) error {
			return nil
		}
		broker := &mockBrokerPort{}
		builder := NewSimpleConsumerBuilder(testTopicName, process)

		adaptedConsumer := builder.Build(broker)

		if adaptedConsumer == nil {
			t.Error("Build should return consumer")
		}
		if adaptedConsumer.Context() == nil {
			t.Error("Build should return context via Context()")
		}
		if adaptedConsumer.Logger() == nil {
			t.Error("Build should return logger via Logger()")
		}
		// type assert to SimpleConsumer to access TopicName
		simpleConsumer, ok := adaptedConsumer.(*SimpleConsumer)
		if !ok {
			t.Fatal("expected SimpleConsumer type")
		}
		if simpleConsumer.TopicName() != testTopicName {
			t.Errorf("TopicName() = %q, want %q", simpleConsumer.TopicName(), testTopicName)
		}
	})

	t.Run("returned context is cancellable", func(t *testing.T) {
		builder := NewSimpleConsumerBuilder("test", nil)
		broker := &mockBrokerPort{}

		adaptedConsumer := builder.Build(broker)

		// shutdown should cancel the context
		simpleConsumer, ok := adaptedConsumer.(*SimpleConsumer)
		if !ok {
			t.Fatal("expected SimpleConsumer type")
		}
		simpleConsumer.cancel()

		select {
		case <-adaptedConsumer.Context().Done():
			// expected
		case <-time.After(100 * time.Millisecond):
			t.Error("context should be cancelled after cancel() is called")
		}
	})
}

// --- SimpleConsumer Tests ---

func TestSimpleConsumer_Subscribe(t *testing.T) {
	t.Run("calls broker Subscribe", func(t *testing.T) {
		broker := &mockBrokerPort{}
		consumer := &SimpleConsumer{
			broker: broker,
			ctx:    context.Background(),
			logger: &capturingLogger{},
		}

		err := consumer.Subscribe()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !broker.subscribeCalled {
			t.Error("expected broker.Subscribe to be called")
		}

		// cleanup
		consumer.running.Store(false)
		<-consumer.done
	})

	t.Run("returns error if broker Subscribe fails", func(t *testing.T) {
		expectedErr := errors.New("subscribe failed")
		broker := &mockBrokerPort{subscribeErr: expectedErr}
		consumer := &SimpleConsumer{
			broker: broker,
			ctx:    context.Background(),
		}

		err := consumer.Subscribe()

		if err == nil {
			t.Error("expected error")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})
}

func TestSimpleConsumer_Shutdown(t *testing.T) {
	t.Run("stops polling loop and unsubscribes", func(t *testing.T) {
		broker := &mockBrokerPort{}
		ctx, cancel := context.WithCancel(context.Background())
		consumer := &SimpleConsumer{
			broker: broker,
			ctx:    ctx,
			cancel: cancel,
			logger: &capturingLogger{},
		}

		// start polling
		_ = consumer.Subscribe()

		// shutdown
		err := consumer.Shutdown()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !broker.unsubscribeCalled {
			t.Error("expected broker.Unsubscribe to be called")
		}
		if consumer.running.Load() {
			t.Error("consumer should not be running after shutdown")
		}
	})

	t.Run("returns error if broker Unsubscribe fails", func(t *testing.T) {
		expectedErr := errors.New("unsubscribe failed")
		broker := &mockBrokerPort{unsubscribeErr: expectedErr}
		ctx, cancel := context.WithCancel(context.Background())
		consumer := &SimpleConsumer{
			broker: broker,
			ctx:    ctx,
			cancel: cancel,
			logger: &capturingLogger{},
			done:   make(chan struct{}),
		}
		close(consumer.done)

		err := consumer.Shutdown()

		if err == nil {
			t.Error("expected error")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})
}

func TestSimpleConsumer_PollLoop(t *testing.T) {
	t.Run("processes messages and collects metrics", func(t *testing.T) {
		var processCount atomic.Int32
		process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
			processCount.Add(1)
			msg.AddCustomTraits(1 << 10) // custom trait
			return nil
		}

		record := &kgo.Record{
			Key:       []byte("key"),
			Partition: 0,
			Offset:    100,
		}

		broker := &mockBrokerPort{
			pollRecords: []*kgo.Record{record, nil, nil, nil, nil}, // one message then nils
		}

		ctx, cancel := context.WithCancel(context.Background())
		consumer := &SimpleConsumer{
			broker:  broker,
			process: process,
			ctx:     ctx,
			cancel:  cancel,
			logger:  &capturingLogger{},
			metrics: make([]nexus.Metrics, 0, 10),
		}

		_ = consumer.Subscribe()

		// wait for message to be processed
		time.Sleep(50 * time.Millisecond)

		cancel()
		<-consumer.done

		if processCount.Load() < 1 {
			t.Error("expected at least one message to be processed")
		}

		metrics := consumer.Metrics()
		if len(metrics) < 1 {
			t.Fatal("expected at least one metric")
		}

		if metrics[0].Partition != 0 {
			t.Errorf("metrics.Partition = %d, want 0", metrics[0].Partition)
		}
		if metrics[0].Offset != 100 {
			t.Errorf("metrics.Offset = %d, want 100", metrics[0].Offset)
		}
		// check custom trait was recorded
		if metrics[0].Traits&(1<<10) == 0 {
			t.Error("expected custom trait to be recorded in metrics")
		}
	})

	t.Run("continues on poll error", func(t *testing.T) {
		logger := &capturingLogger{}
		broker := &mockBrokerPort{
			pollErr: errors.New("poll error"),
		}

		ctx, cancel := context.WithCancel(context.Background())
		consumer := &SimpleConsumer{
			broker: broker,
			ctx:    ctx,
			cancel: cancel,
			logger: logger,
		}

		_ = consumer.Subscribe()

		// let it try to poll a few times
		time.Sleep(50 * time.Millisecond)

		cancel()
		<-consumer.done

		if !logger.errorCalled {
			t.Error("expected error to be logged")
		}
	})
}

func TestSimpleConsumer_TriggerRebalance(t *testing.T) {
	t.Run("calls broker AckRebalance", func(t *testing.T) {
		broker := &mockBrokerPort{}
		consumer := &SimpleConsumer{broker: broker}

		err := consumer.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !broker.ackRebalanceCalled {
			t.Error("expected broker.AckRebalance to be called")
		}
	})

	t.Run("returns error from broker", func(t *testing.T) {
		expectedErr := errors.New("ack error")
		broker := &mockBrokerPort{ackRebalanceErr: expectedErr}
		consumer := &SimpleConsumer{broker: broker}

		err := consumer.TriggerRebalance(nexus.Assign, nil)

		if err == nil {
			t.Error("expected error")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})
}

func TestSimpleConsumer_Metrics(t *testing.T) {
	t.Run("returns copy of metrics", func(t *testing.T) {
		consumer := &SimpleConsumer{
			metrics: []nexus.Metrics{
				{Partition: 0, Offset: 100},
				{Partition: 1, Offset: 200},
			},
		}

		metrics := consumer.Metrics()

		if len(metrics) != 2 {
			t.Fatalf("expected 2 metrics, got %d", len(metrics))
		}

		// verify it's a copy by modifying original
		consumer.metrics[0].Offset = 999
		if metrics[0].Offset == 999 {
			t.Error("Metrics() should return a copy, not reference")
		}
	})
}

func TestSimpleConsumer_MetricsCount(t *testing.T) {
	consumer := &SimpleConsumer{
		metrics: []nexus.Metrics{
			{Partition: 0},
			{Partition: 1},
			{Partition: 2},
		},
	}

	count := consumer.MetricsCount()

	if count != 3 {
		t.Errorf("MetricsCount() = %d, want 3", count)
	}
}

// --- mockBrokerPort for testing ---

type mockBrokerPort struct {
	subscribeCalled    bool
	subscribeErr       error
	unsubscribeCalled  bool
	unsubscribeErr     error
	ackRebalanceCalled bool
	ackRebalanceErr    error
	pollErr            error
	pollRecords        []*kgo.Record
	pollIndex          int
	pollMu             sync.Mutex
	commitCalled       bool
}

func (m *mockBrokerPort) Subscribe() error {
	m.subscribeCalled = true
	return m.subscribeErr
}

func (m *mockBrokerPort) Unsubscribe() error {
	m.unsubscribeCalled = true
	return m.unsubscribeErr
}

func (m *mockBrokerPort) Poll(_ time.Duration) (*kgo.Record, bool, error) {
	if m.pollErr != nil {
		return nil, false, m.pollErr
	}

	m.pollMu.Lock()
	defer m.pollMu.Unlock()

	if m.pollIndex < len(m.pollRecords) {
		record := m.pollRecords[m.pollIndex]
		m.pollIndex++
		if record != nil {
			return record, true, nil
		}
	}
	return nil, false, nil
}

func (m *mockBrokerPort) ExtractEnvelope(record *kgo.Record) nexus.Envelope {
	return nexus.Envelope{
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       string(record.Key),
		Ctx:       context.Background(),
	}
}

func (m *mockBrokerPort) CommitOffsets(_ []*nexus.Message[*kgo.Record]) ([]*nexus.Message[*kgo.Record], error) {
	m.commitCalled = true
	return nil, nil
}

func (m *mockBrokerPort) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	m.ackRebalanceCalled = true
	return m.ackRebalanceErr
}

func (m *mockBrokerPort) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

func (m *mockBrokerPort) ConsumerGroup() string {
	return ""
}
