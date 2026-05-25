// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-nexus/nexus"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

// skipIfShort skips integration tests when running with -short flag.
func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

// custom traits for identifying which consumer processed each message (bits 10+)
const (
	TraitConsumer1 nexus.Traits = 1 << 10
	TraitConsumer2 nexus.Traits = 1 << 11
)

const (
	numPartitions = 4
	messageCount  = 100
)

// --- Test Helpers ---

func startKafka(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := kafkacontainer.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafkacontainer.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container (is Docker running?): %v", err)
	}

	brokers, err := container.Brokers(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get brokers: %v", err)
	}

	// t.Cleanup runs AFTER all defers, ensuring consumers shut down before container.
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		_ = container.Terminate(ctx)
	})

	return brokers[0]
}

func createTopic(t *testing.T, bootstrapServers, topic string, partitions int32) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers))
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	resp, err := admin.CreateTopics(context.Background(), partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Fatalf("failed to create topic %s: %v", r.Topic, r.Err)
		}
	}

	// wait for topic to be ready
	time.Sleep(500 * time.Millisecond)
}

func publishMessages(t *testing.T, bootstrapServers, topic string, count int) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers))
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer client.Close()

	records := make([]*kgo.Record, count)
	for i := range count {
		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%04d", i)),
			Value: []byte(fmt.Sprintf("value-%04d", i)),
		}
	}

	results := client.ProduceSync(context.Background(), records...)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("failed to produce message %d: %v", i, r.Err)
		}
	}

	t.Logf("published %d messages to %s", count, topic)
}

type consumerHandle struct {
	consumer *SimpleConsumer
	adapter  *franzadapter.Adapter
}

func createConsumer(
	ctx context.Context,
	t *testing.T,
	bootstrapServers, topic, groupID string,
	consumerTrait nexus.Traits,
) *consumerHandle {
	return createConsumerWithDelay(ctx, t, bootstrapServers, topic, groupID, consumerTrait, 0)
}

func createConsumerWithDelay(
	ctx context.Context,
	t *testing.T,
	bootstrapServers, topic, groupID string,
	consumerTrait nexus.Traits,
	processingDelay time.Duration,
) *consumerHandle {
	t.Helper()

	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		if processingDelay > 0 {
			time.Sleep(processingDelay)
		}
		// mark which consumer processed this message
		nexus.SetTraits(&msg.Traits, consumerTrait)
		return nil
	}

	adapter := franzadapter.New(ctx, groupID, bootstrapServers)

	builder := NewSimpleConsumerBuilder(topic, process)
	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	return &consumerHandle{
		consumer: sc,
		adapter:  adapter,
	}
}

func waitForMessages(consumers []*consumerHandle, expected int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		total := 0
		for _, c := range consumers {
			total += c.consumer.MetricsCount()
		}
		if total >= expected {
			return total
		}
		time.Sleep(100 * time.Millisecond)
	}

	total := 0
	for _, c := range consumers {
		total += c.consumer.MetricsCount()
	}
	return total
}

func countByTrait(consumers []*consumerHandle) map[nexus.Traits]int {
	counts := make(map[nexus.Traits]int)
	for _, c := range consumers {
		for _, m := range c.consumer.Metrics() {
			if m.Traits&TraitConsumer1 != 0 {
				counts[TraitConsumer1]++
			}
			if m.Traits&TraitConsumer2 != 0 {
				counts[TraitConsumer2]++
			}
		}
	}
	return counts
}

// --- Tests ---

func TestSingleConsumer_CooperativeSticky(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-coop"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumer(ctx, t, bootstrapServers, topic, "group-single-coop", TraitConsumer1)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	counts := countByTrait([]*consumerHandle{handle})
	if counts[TraitConsumer1] != messageCount {
		t.Errorf("consumer 1 should have trait on all %d messages, got %d",
			messageCount, counts[TraitConsumer1])
	}

	t.Logf("single consumer (cooperative-sticky): received %d messages", total)
}

func TestTwoConsumers_CooperativeSticky(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-two-coop"
	groupID := "group-two-coop"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// publish messages first so they're ready when consumers subscribe
	publishMessages(t, bootstrapServers, topic, messageCount)

	// start first consumer
	handle1 := createConsumer(ctx, t, bootstrapServers, topic, groupID, TraitConsumer1)
	defer func() { _ = handle1.consumer.Shutdown() }()

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}

	// wait for consumer 1 to process at least some messages before adding consumer 2
	waitForMessages([]*consumerHandle{handle1}, 10, 30*time.Second)

	// start second consumer (triggers cooperative rebalance)
	handle2 := createConsumer(ctx, t, bootstrapServers, topic, groupID, TraitConsumer2)
	defer func() { _ = handle2.consumer.Shutdown() }()

	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// wait for all messages
	handles := []*consumerHandle{handle1, handle2}
	total := waitForMessages(handles, messageCount, 30*time.Second)
	if total < messageCount {
		t.Errorf("expected at least %d messages, got %d", messageCount, total)
	}

	counts := countByTrait(handles)
	t.Logf("two consumers (cooperative-sticky): consumer1=%d, consumer2=%d, total=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// consumer 1 should have processed some messages (it started first)
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed some messages")
	}
}

func TestConsumerShutdown_Rebalance(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-shutdown-rebalance"
	groupID := "group-shutdown-rebalance"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// start first consumer with processing delay to prevent it consuming everything
	handle1 := createConsumerWithDelay(
		ctx, t, bootstrapServers, topic, groupID, TraitConsumer1, 20*time.Millisecond)

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}

	// publish messages
	publishMessages(t, bootstrapServers, topic, messageCount)

	// wait for consumer 1 to process some messages
	waitForMessages([]*consumerHandle{handle1}, 20, 30*time.Second)

	// shutdown consumer 1
	if err := handle1.consumer.Shutdown(); err != nil {
		t.Fatalf("consumer 1 shutdown failed: %v", err)
	}

	// start consumer 2 - should take over all partitions
	handle2 := createConsumer(ctx, t, bootstrapServers, topic, groupID, TraitConsumer2)
	defer func() { _ = handle2.consumer.Shutdown() }()

	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// publish additional messages to ensure consumer 2 has work to do
	additionalMessages := 50
	publishMessages(t, bootstrapServers, topic, additionalMessages)

	// wait for all messages (original + additional)
	expectedTotal := messageCount + additionalMessages
	handles := []*consumerHandle{handle1, handle2}
	total := waitForMessages(handles, expectedTotal, 30*time.Second)
	if total < expectedTotal {
		t.Errorf("expected at least %d messages, got %d", expectedTotal, total)
	}

	counts := countByTrait(handles)
	t.Logf("shutdown rebalance: consumer1=%d, consumer2=%d, total=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// consumer 1 should have processed some messages before shutdown
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed some messages before shutdown")
	}
	// consumer 2 should have taken over and processed remaining messages
	if counts[TraitConsumer2] == 0 {
		t.Error("consumer 2 should have processed messages after taking over")
	}
}

func TestPublishWhileConsuming(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-publish-while-consuming"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// start consumer first
	handle := createConsumer(ctx, t, bootstrapServers, topic, "group-publish-while", TraitConsumer1)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// wait a bit for consumer to be ready
	time.Sleep(2 * time.Second)

	// publish messages while consumer is running
	publishMessages(t, bootstrapServers, topic, messageCount)

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	t.Logf("publish while consuming: received %d messages", total)
}

// createConsumerWithOpts creates a consumer with custom kgo.Opt configuration.
// Use this to override balancer strategy, set SASL/TLS, or pass timeout/fetch
// options through to the underlying kgo.Client. Caller-supplied options are
// applied AFTER the adapter's defaults, so they override them.
func createConsumerWithOpts(
	ctx context.Context,
	t *testing.T,
	bootstrapServers, topic, groupID string,
	consumerTrait nexus.Traits,
	extraOpts ...kgo.Opt,
) *consumerHandle {
	t.Helper()

	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		nexus.SetTraits(&msg.Traits, consumerTrait)
		return nil
	}

	adapter := franzadapter.NewWithOptions(ctx, groupID, []string{bootstrapServers}, extraOpts...)

	builder := NewSimpleConsumerBuilder(topic, process)
	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	return &consumerHandle{
		consumer: sc,
		adapter:  adapter,
	}
}

// --- Balancer strategy tests ---

func TestSingleConsumer_Range(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-range"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-single-range", TraitConsumer1,
		kgo.Balancers(kgo.RangeBalancer()),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	counts := countByTrait([]*consumerHandle{handle})
	if counts[TraitConsumer1] != messageCount {
		t.Errorf("consumer 1 should have trait on all %d messages, got %d",
			messageCount, counts[TraitConsumer1])
	}

	t.Logf("single consumer (range): received %d messages", total)
}

func TestSingleConsumer_RoundRobin(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-roundrobin"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-single-rr", TraitConsumer1,
		kgo.Balancers(kgo.RoundRobinBalancer()),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	t.Logf("single consumer (roundrobin): received %d messages", total)
}

func TestTwoConsumers_Range(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-two-range"
	groupID := "group-two-range"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// Range is an eager-protocol balancer: both consumers must join the group
	// before we publish, so partitions are distributed at first assignment.
	handle1 := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, groupID, TraitConsumer1,
		kgo.Balancers(kgo.RangeBalancer()),
	)
	defer func() { _ = handle1.consumer.Shutdown() }()

	handle2 := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, groupID, TraitConsumer2,
		kgo.Balancers(kgo.RangeBalancer()),
	)
	defer func() { _ = handle2.consumer.Shutdown() }()

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}
	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// Active readiness probe: each iteration drops one message into every
	// partition and polls for evidence that BOTH consumers are drawing
	// traffic. Continuous emission (rather than a single upfront batch) is
	// required because the range balancer's eager rebalance takes a few
	// seconds: probes published before c2 joins the group all land on c1.
	// Re-emitting until both consumers see traffic adapts to broker speed
	// and surfaces a clear failure if the split never happens.
	//
	// ManualPartitioner is required for Record.Partition to be honoured;
	// the default StickyKeyPartitioner ignores it and routes all records
	// to one (sticky) partition.
	handles := []*consumerHandle{handle1, handle2}

	probeClient, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("failed to create probe producer: %v", err)
	}
	defer probeClient.Close()

	emitProbeBatch := func() {
		for p := int32(0); p < numPartitions; p++ {
			probeClient.Produce(ctx, &kgo.Record{
				Topic:     topic,
				Partition: p,
				Key:       []byte("probe"),
				Value:     []byte("probe"),
			}, nil)
		}
		_ = probeClient.Flush(ctx)
	}

	readyDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(readyDeadline) {
		emitProbeBatch()
		time.Sleep(500 * time.Millisecond)
		c := countByTrait(handles)
		if c[TraitConsumer1] > 0 && c[TraitConsumer2] > 0 {
			break
		}
	}
	if c := countByTrait(handles); c[TraitConsumer1] == 0 || c[TraitConsumer2] == 0 {
		t.Fatalf("consumers not both receiving traffic after 30s: c1=%d c2=%d "+
			"(either range balancer didn't split partitions, or probe producer didn't reach all partitions - "+
			"check kgo.RecordPartitioner is ManualPartitioner)",
			c[TraitConsumer1], c[TraitConsumer2])
	}

	// Count probe messages already consumed so the post-publish wait can
	// distinguish probes from the real payload.
	probesAlreadyConsumed := 0
	for _, h := range handles {
		probesAlreadyConsumed += h.consumer.MetricsCount()
	}

	publishMessages(t, bootstrapServers, topic, messageCount)

	total := waitForMessages(handles, probesAlreadyConsumed+messageCount, 30*time.Second)
	if total < probesAlreadyConsumed+messageCount {
		t.Errorf("expected at least %d messages (incl. %d probes), got %d",
			probesAlreadyConsumed+messageCount, probesAlreadyConsumed, total)
	}

	counts := countByTrait(handles)
	t.Logf("two consumers (range): consumer1=%d, consumer2=%d, total=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// Readiness probe already proved both consumers are receiving; these
	// assertions are belt-and-braces against late-rebalance regressions.
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed some messages")
	}
	if counts[TraitConsumer2] == 0 {
		t.Error("consumer 2 should have processed some messages")
	}
}

// --- Rebalance phase transitions across all balancer strategies ---

// TestRebalancePattern_PhaseTransitions verifies the rebalance pattern for all
// three group balancer strategies:
//
//	Phase 1 (0-5s):   consumer 1 only            -> c1 dominates
//	Phase 2 (5-10s):  consumer 1 + consumer 2    -> both process messages
//	Phase 3 (10-15s): consumer 2 only (c1 shut)  -> c2 dominates
//
// A continuous publisher fires at 50ms intervals throughout. Messages are
// keyed with their sequence number so we can map exactly which consumer
// processed each message and bucket the result by phase.
func TestRebalancePattern_PhaseTransitions(t *testing.T) {
	skipIfShort(t)
	strategies := map[string]kgo.GroupBalancer{
		"range":              kgo.RangeBalancer(),
		"roundrobin":         kgo.RoundRobinBalancer(),
		"cooperative-sticky": kgo.CooperativeStickyBalancer(),
	}

	for name, balancer := range strategies {
		t.Run(name, func(t *testing.T) {
			runRebalancePhaseTest(t, name, balancer)
		})
	}
}

func runRebalancePhaseTest(t *testing.T, strategy string, balancer kgo.GroupBalancer) {
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := fmt.Sprintf("test-rebalance-%s", strategy)
	groupID := fmt.Sprintf("group-rebalance-%s", strategy)
	createTopic(t, bootstrapServers, topic, numPartitions)

	const (
		phaseDuration   = 5 * time.Second
		publishInterval = 50 * time.Millisecond
	)

	var publishMu sync.Mutex
	phase1Messages := make(map[int]bool)
	phase2Messages := make(map[int]bool)
	phase3Messages := make(map[int]bool)

	var processMu sync.Mutex
	processedBy := make(map[int]nexus.Traits)

	makeProcessFn := func(trait nexus.Traits) nexus.ProcessMessage[*kgo.Record] {
		return func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
			var msgIdx int
			if _, err := fmt.Sscanf(msg.Key, "key-%d", &msgIdx); err == nil {
				processMu.Lock()
				processedBy[msgIdx] |= trait // OR to handle duplicates from rebalance
				processMu.Unlock()
			}
			nexus.SetTraits(&msg.Traits, trait)
			return nil
		}
	}

	createConsumerForPhase := func(process nexus.ProcessMessage[*kgo.Record]) *consumerHandle {
		adapter := franzadapter.NewWithOptions(
			ctx, groupID, []string{bootstrapServers},
			kgo.Balancers(balancer),
		)

		builder := NewSimpleConsumerBuilder(topic, process)
		consumer, err := adapter.CreateConsumer(builder)
		if err != nil {
			t.Fatalf("failed to create consumer: %v", err)
		}
		sc, ok := consumer.(*SimpleConsumer)
		if !ok {
			t.Fatal("expected *SimpleConsumer")
		}
		return &consumerHandle{consumer: sc, adapter: adapter}
	}

	// Standalone producer client (no consumer group) for steady publishing.
	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers))
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	stopPublishing := make(chan struct{})
	publishDone := make(chan struct{})
	var msgIndex atomic.Int64

	go func() {
		defer close(publishDone)
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopPublishing:
				if err := producer.Flush(context.Background()); err != nil {
					t.Logf("producer flush error during stop: %v", err)
				}
				return
			case <-ticker.C:
				idx := msgIndex.Load()
				key := fmt.Sprintf("key-%04d", idx)
				value := fmt.Sprintf("value-%04d", idx)

				producer.Produce(context.Background(), &kgo.Record{
					Topic: topic,
					Key:   []byte(key),
					Value: []byte(value),
				}, nil)

				msgIndex.Add(1)
			}
		}
	}()

	// Phase 1: consumer 1 only
	t.Logf("[%s] phase 1: starting consumer 1...", strategy)
	handle1 := createConsumerForPhase(makeProcessFn(TraitConsumer1))

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}

	time.Sleep(3 * time.Second) // let consumer 1 receive initial assignment (+50% slack vs 2s)

	phase1Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase1End := msgIndex.Load()

	publishMu.Lock()
	for i := phase1Start; i < phase1End; i++ {
		phase1Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 1: published messages %d-%d (%d total)",
		strategy, phase1Start, phase1End-1, phase1End-phase1Start)

	// Phase 2: add consumer 2
	t.Logf("[%s] phase 2: starting consumer 2...", strategy)
	handle2 := createConsumerForPhase(makeProcessFn(TraitConsumer2))

	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	time.Sleep(4500 * time.Millisecond) // let rebalance complete (+50% slack vs 3s)

	phase2Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase2End := msgIndex.Load()

	publishMu.Lock()
	for i := phase2Start; i < phase2End; i++ {
		phase2Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 2: published messages %d-%d (%d total)",
		strategy, phase2Start, phase2End-1, phase2End-phase2Start)

	// Phase 3: shutdown consumer 1
	t.Logf("[%s] phase 3: shutting down consumer 1...", strategy)
	_ = handle1.consumer.Shutdown()

	time.Sleep(4500 * time.Millisecond) // let rebalance complete after shutdown (+50% slack vs 3s)

	phase3Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase3End := msgIndex.Load()

	publishMu.Lock()
	for i := phase3Start; i < phase3End; i++ {
		phase3Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 3: published messages %d-%d (%d total)",
		strategy, phase3Start, phase3End-1, phase3End-phase3Start)

	close(stopPublishing)
	<-publishDone
	if err := producer.Flush(context.Background()); err != nil {
		t.Logf("final flush error: %v", err)
	}

	totalPublished := int(msgIndex.Load())
	t.Logf("[%s] total published: %d messages", strategy, totalPublished)

	// Wait for all messages to be consumed (or timeout, +50% slack vs 60s)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		processMu.Lock()
		consumed := len(processedBy)
		processMu.Unlock()
		if consumed >= totalPublished {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_ = handle2.consumer.Shutdown()

	processMu.Lock()
	defer processMu.Unlock()

	var phase1C1, phase1C2, phase1Both int
	var phase2C1, phase2C2, phase2Both int
	var phase3C1, phase3C2, phase3Both int

	for idx, traits := range processedBy {
		hasC1 := traits&TraitConsumer1 != 0
		hasC2 := traits&TraitConsumer2 != 0

		if phase1Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase1Both++
			case hasC1:
				phase1C1++
			case hasC2:
				phase1C2++
			}
		}
		if phase2Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase2Both++
			case hasC1:
				phase2C1++
			case hasC2:
				phase2C2++
			}
		}
		if phase3Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase3Both++
			case hasC1:
				phase3C1++
			case hasC2:
				phase3C2++
			}
		}
	}

	t.Logf("[%s] phase 1 results: c1=%d, c2=%d, both=%d", strategy, phase1C1, phase1C2, phase1Both)
	t.Logf("[%s] phase 2 results: c1=%d, c2=%d, both=%d", strategy, phase2C1, phase2C2, phase2Both)
	t.Logf("[%s] phase 3 results: c1=%d, c2=%d, both=%d", strategy, phase3C1, phase3C2, phase3Both)

	// Phase 1: consumer 1 should dominate (consumer 2 hadn't joined yet)
	phase1Total := phase1C1 + phase1C2 + phase1Both
	if phase1Total > 0 && phase1C1 < phase1Total/2 {
		t.Errorf("[%s] phase 1: expected consumer 1 to dominate, got c1=%d, c2=%d, both=%d",
			strategy, phase1C1, phase1C2, phase1Both)
	}

	// Phase 2: both consumers should process some (partitions are split)
	phase2Total := phase2C1 + phase2C2 + phase2Both
	if phase2Total > 0 {
		if phase2C1+phase2Both == 0 {
			t.Errorf("[%s] phase 2: consumer 1 should have processed some messages", strategy)
		}
		if phase2C2+phase2Both == 0 {
			t.Errorf("[%s] phase 2: consumer 2 should have processed some messages", strategy)
		}
	}

	// Phase 3: consumer 2 should dominate (consumer 1 was shut down)
	phase3Total := phase3C1 + phase3C2 + phase3Both
	if phase3Total > 0 && phase3C2 < phase3Total/2 {
		t.Errorf("[%s] phase 3: expected consumer 2 to dominate, got c1=%d, c2=%d, both=%d",
			strategy, phase3C1, phase3C2, phase3Both)
	}

	consumed := len(processedBy)
	if consumed < totalPublished {
		t.Errorf("[%s] message loss: published %d, consumed %d", strategy, totalPublished, consumed)
	} else {
		t.Logf("[%s] all %d messages consumed successfully", strategy, consumed)
	}
}

// --- Config option passthrough tests ---
//
// These tests verify that the user-supplied kgo.Opt values reach the underlying
// kgo.Client without breaking consumer creation or message consumption. They do
// NOT verify the option's runtime effect (e.g. that SessionTimeout actually
// changes broker heartbeat behaviour) - that's the job of franz-go's own tests.
// What they catch here is "adapter dropped the option" or "adapter mis-wraps
// the option causing consumer failure to construct".

// TestConfigOptions_SessionTimeout verifies kgo.SessionTimeout passes through.
func TestConfigOptions_SessionTimeout(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-session-timeout"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-session-timeout", TraitConsumer1,
		kgo.SessionTimeout(45*time.Second),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}
	t.Logf("session timeout config: received %d messages", total)
}

// TestConfigOptions_RebalanceTimeout verifies kgo.RebalanceTimeout passes
// through (franz-go analog of confluent-kafka-go's max.poll.interval.ms).
func TestConfigOptions_RebalanceTimeout(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-rebalance-timeout"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-rebalance-timeout", TraitConsumer1,
		kgo.RebalanceTimeout(5*time.Minute),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}
	t.Logf("rebalance timeout config: received %d messages", total)
}

// TestConfigOptions_FetchSettings verifies fetch-related kgo opts pass through.
func TestConfigOptions_FetchSettings(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-fetch-settings"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 50)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-fetch-settings", TraitConsumer1,
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(1<<20),          // 1 MiB
		kgo.FetchMaxPartitionBytes(1<<18), // 256 KiB
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 50, 30*time.Second)
	if total != 50 {
		t.Errorf("expected 50 messages, got %d", total)
	}
	t.Logf("fetch settings config: received %d messages", total)
}

// TestConfigOptions_IsolationLevel verifies kgo.FetchIsolationLevel passes through.
func TestConfigOptions_IsolationLevel(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-isolation-level"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-isolation", TraitConsumer1,
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}
	t.Logf("isolation level config: received %d messages", total)
}

// TestConfigOptions_ClientID verifies kgo.ClientID passes through.
func TestConfigOptions_ClientID(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-client-id"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-client-id", TraitConsumer1,
		kgo.ClientID("test-consumer-instance-1"),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}
	t.Logf("client.id config: received %d messages", total)
}

// TestConfigOptions_MaxBufferedRecords verifies kgo.MaxBufferedRecords passes
// through (franz-go analog of confluent-kafka-go's queued.min/max.messages).
func TestConfigOptions_MaxBufferedRecords(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-max-buffered"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 100)

	handle := createConsumerWithOpts(
		ctx, t, bootstrapServers, topic, "group-max-buffered", TraitConsumer1,
		kgo.MaxBufferedRecords(1000),
	)
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, 100, 30*time.Second)
	if total != 100 {
		t.Errorf("expected 100 messages, got %d", total)
	}
	t.Logf("max buffered records config: received %d messages", total)
}

// --- SASL/OAuth wiring verification ---

// TestSetSASL_OAuthOption verifies the SASL/OAUTHBEARER mechanism wires in
// without panicking. Mirrors kafka's TestSetOAuthTokenRefresh_CallbackRegistration:
// we don't actually authenticate (the test broker isn't OAuth-configured), we
// just verify the adapter accepts the option and that consumer construction
// completes the option-application step before any connection attempt.
func TestSetSASL_OAuthOption(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	// Build an adapter declaring SASL/OAUTHBEARER with a token callback.
	// We never call CreateConsumer with this adapter because the broker
	// would reject the SASL handshake - the point is to exercise option
	// acceptance, mirroring the kafka equivalent.
	tokenFn := func(_ context.Context) (oauth.Auth, error) {
		return oauth.Auth{Token: "test-token"}, nil
	}

	adapter := franzadapter.NewWithOptions(
		ctx, "group-sasl-oauth", []string{bootstrapServers},
		kgo.SASL(oauth.Oauth(tokenFn)),
	)
	if adapter == nil {
		t.Fatal("NewWithOptions returned nil adapter")
	}

	t.Log("SASL/OAuth option: adapter accepted oauth.Oauth(callback) mechanism")
}

// --- Direct client access (franz equivalent of confluent's underlying-consumer exposure) ---

// TestNewCustom_DirectClientAccess verifies the franz-go equivalent of
// confluent-kafka-go's adapter.ConfluentConsumer(): the NewCustom() + SetClient()
// path lets the host application create and fully own the underlying *kgo.Client,
// while the adapter participates in the rebalance protocol via its callbacks.
//
// End-to-end: messages produced by a separate client are consumed through the
// adapter-wrapped, user-owned kgo.Client.
func TestNewCustom_DirectClientAccess(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-newcustom"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	adapter := franzadapter.NewCustom()

	// Caller builds the client directly, wiring in the adapter's rebalance
	// callbacks - matches the documented NewCustom contract.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers),
		kgo.ConsumerGroup("group-newcustom"),
		kgo.ConsumeTopics(topic),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(adapter.OnAssigned),
		kgo.OnPartitionsRevoked(adapter.OnRevoked),
		kgo.OnPartitionsLost(adapter.OnLost),
	)
	if err != nil {
		t.Fatalf("failed to create kgo.Client: %v", err)
	}
	adapter.SetClient(client)

	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}
	builder := NewSimpleConsumerBuilder(topic, process)
	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		t.Fatalf("CreateConsumer failed: %v", err)
	}

	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}
	defer func() { _ = sc.Shutdown() }()

	if err := sc.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	handle := &consumerHandle{consumer: sc, adapter: adapter}
	total := waitForMessages([]*consumerHandle{handle}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}
	t.Logf("NewCustom + SetClient direct access: received %d messages", total)
}

// --- Message delivery guarantee ---

// TestAllMessagesDelivered_NoLoss verifies the adapter delivers every produced
// message to user-supplied process callback at least once. Uses (partition,
// offset) as a stable unique key so duplicate deliveries (which are allowed)
// don't inflate the count.
//
// This is the strongest correctness guarantee in the suite: if it fails, the
// adapter is silently dropping messages somewhere in poll -> process -> commit.
func TestAllMessagesDelivered_NoLoss(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-no-loss"
	groupID := "group-no-loss"
	msgCount := 500
	createTopic(t, bootstrapServers, topic, numPartitions)

	var mu sync.Mutex
	seen := make(map[string]bool)

	process := func(_ context.Context, msg *nexus.Message[*kgo.Record]) error {
		key := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
		mu.Lock()
		seen[key] = true
		mu.Unlock()
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	adapter := franzadapter.NewWithOptions(
		ctx, groupID, []string{bootstrapServers},
		kgo.Balancers(kgo.RangeBalancer()),
	)

	builder := NewSimpleConsumerBuilder(topic, process)
	consumer, err := adapter.CreateConsumer(builder)
	if err != nil {
		t.Fatalf("CreateConsumer failed: %v", err)
	}
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}
	defer func() { _ = sc.Shutdown() }()

	publishMessages(t, bootstrapServers, topic, msgCount)

	if err := sc.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(seen)
		mu.Unlock()
		if count >= msgCount {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	finalCount := len(seen)
	mu.Unlock()

	if finalCount != msgCount {
		t.Errorf("message loss detected: expected %d unique messages, got %d", msgCount, finalCount)
	} else {
		t.Logf("all %d messages delivered without loss", msgCount)
	}
}
