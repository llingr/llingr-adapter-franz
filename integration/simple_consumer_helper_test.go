// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// defaultPollTimeout is the broker poll timeout for each iteration.
	defaultPollTimeout = 100 * time.Millisecond

	// initialMetricsCapacity is the initial slice capacity for collected metrics.
	initialMetricsCapacity = 1000

	// testTopicName is the placeholder topic used by mock-based unit tests.
	testTopicName = "test-topic"
)

// mockLogger is a no-op logger used by mock-based SimpleConsumer unit tests.
type mockLogger struct{}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (m *mockLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Error(_ context.Context, _ string, _ ...any) {}

// capturingLogger records log calls for verification in tests that need
// to assert which level was logged.
type capturingLogger struct {
	debugCalled  bool
	infoCalled   bool
	warnCalled   bool
	errorCalled  bool
	lastInfoMsg  string
	lastErrorMsg string
}

func (l *capturingLogger) Debug(_ context.Context, _ string, _ ...any) {
	l.debugCalled = true
}

func (l *capturingLogger) Info(_ context.Context, format string, _ ...any) {
	l.infoCalled = true
	l.lastInfoMsg = format
}

func (l *capturingLogger) Warn(_ context.Context, _ string, _ ...any) {
	l.warnCalled = true
}

func (l *capturingLogger) Error(_ context.Context, format string, _ ...any) {
	l.errorCalled = true
	l.lastErrorMsg = format
}

// SimpleConsumer is a minimal test consumer: poll -> process -> commit.
// Ignores processing errors, collects metrics to a slice.
type SimpleConsumer struct {
	broker    nexus.BrokerPort[*kgo.Record]
	process   nexus.ProcessMessage[*kgo.Record]
	topicName string
	ctx       context.Context
	cancel    context.CancelFunc
	logger    nexus.Logger

	metrics []nexus.Metrics
	mu      sync.Mutex

	running atomic.Bool
	done    chan struct{}
}

// Subscribe starts the polling loop.
func (c *SimpleConsumer) Subscribe() error {
	if err := c.broker.Subscribe(); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	c.done = make(chan struct{})
	c.running.Store(true)
	go c.pollLoop()
	return nil
}

func (c *SimpleConsumer) pollLoop() {
	defer close(c.done)

	for c.running.Load() {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		msg, ok, err := c.broker.Poll(defaultPollTimeout)
		if err != nil {
			c.logger.Error(c.ctx, fmt.Sprintf("poll error: %v", err))
			continue
		}
		if !ok {
			continue
		}

		env := c.broker.ExtractEnvelope(msg)
		start := time.Now()

		nexusMsg := &nexus.Message[*kgo.Record]{
			Partition: env.Partition,
			Offset:    env.Offset,
			Key:       env.Key,
			Payload:   &msg,
		}

		// process - ignore errors
		_ = c.process(c.ctx, nexusMsg)
		processDuration := time.Since(start)

		// commit
		_, _ = c.broker.CommitOffsets([]*nexus.Message[*kgo.Record]{nexusMsg})

		// collect metrics (including any traits set during processing)
		c.mu.Lock()
		c.metrics = append(c.metrics, nexus.Metrics{
			Partition:       env.Partition,
			Offset:          env.Offset,
			ProcessDuration: processDuration,
			Traits:          nexusMsg.Traits,
		})
		c.mu.Unlock()
	}
}

// Shutdown stops the consumer and unsubscribes.
func (c *SimpleConsumer) Shutdown() error {
	c.running.Store(false)
	c.cancel()
	if c.done != nil {
		<-c.done
	}
	if err := c.broker.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	return nil
}

// EmergencyShutdown satisfies the nexus.EmergencyShutdowner contract the
// adapter enforces at wiring time; these tests never bail, so a trip is a
// failure surfaced through Shutdown's normal teardown.
func (c *SimpleConsumer) EmergencyShutdown(error) {
	c.running.Store(false)
	c.cancel()
}

// TopicName returns the configured topic.
func (c *SimpleConsumer) TopicName() string {
	return c.topicName
}

// Context returns the control-plane context for the consumer lifecycle.
func (c *SimpleConsumer) Context() context.Context {
	return c.ctx
}

// Logger returns the operational logger.
func (c *SimpleConsumer) Logger() nexus.Logger {
	return c.logger
}

// TriggerRebalance acknowledges rebalance events.
func (c *SimpleConsumer) TriggerRebalance(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	if err := c.broker.AckRebalance(rt, info); err != nil {
		return fmt.Errorf("ack rebalance: %w", err)
	}
	return nil
}

// Metrics returns a copy of collected metrics.
func (c *SimpleConsumer) Metrics() []nexus.Metrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]nexus.Metrics, len(c.metrics))
	copy(result, c.metrics)
	return result
}

// MetricsCount returns the number of processed messages.
func (c *SimpleConsumer) MetricsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.metrics)
}

// SimpleConsumerBuilder creates SimpleConsumer instances.
type SimpleConsumerBuilder struct {
	topicName string
	process   nexus.ProcessMessage[*kgo.Record]
	logger    nexus.Logger
	ctx       context.Context
}

// NewSimpleConsumerBuilder creates a builder for test consumers.
func NewSimpleConsumerBuilder(
	topicName string,
	process nexus.ProcessMessage[*kgo.Record],
) *SimpleConsumerBuilder {
	return &SimpleConsumerBuilder{
		topicName: topicName,
		process:   process,
		logger:    nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:       context.Background(),
	}
}

// WithLogger sets a custom logger.
func (b *SimpleConsumerBuilder) WithLogger(logger nexus.Logger) *SimpleConsumerBuilder {
	b.logger = logger
	return b
}

// WithContext sets a parent context.
func (b *SimpleConsumerBuilder) WithContext(ctx context.Context) *SimpleConsumerBuilder {
	b.ctx = ctx
	return b
}

// TopicName returns the configured topic.
func (b *SimpleConsumerBuilder) TopicName() string {
	return b.topicName
}

// Build creates the consumer, wiring in the broker port.
func (b *SimpleConsumerBuilder) Build(
	broker nexus.BrokerPort[*kgo.Record],
) nexus.AdaptedConsumer[*kgo.Record] {
	ctx, cancel := context.WithCancel(b.ctx)

	return &SimpleConsumer{
		broker:    broker,
		process:   b.process,
		topicName: b.topicName,
		ctx:       ctx,
		cancel:    cancel,
		logger:    b.logger,
		metrics:   make([]nexus.Metrics, 0, initialMetricsCapacity),
	}
}
