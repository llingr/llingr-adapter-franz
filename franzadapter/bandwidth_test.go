// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBandwidthCollector_FetchBatchAccumulation(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")

	rack := "us-east-1a"
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker-1", Port: 9092, Rack: &rack}

	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{
		NumRecords:        100,
		CompressedBytes:   500,
		UncompressedBytes: 1000,
		CompressionType:   2, // snappy
	})
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{
		NumRecords:        50,
		CompressedBytes:   250,
		UncompressedBytes: 500,
		CompressionType:   2,
	})
	bc.recordFetchBatch(meta, 1, kgo.FetchBatchMetrics{
		NumRecords:        30,
		CompressedBytes:   200,
		UncompressedBytes: 400,
		CompressionType:   3, // lz4
	})

	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(bc.partitions))
	}

	p0 := bc.partitions[0]
	if p0.receivedMessageCount != 150 {
		t.Errorf("partition 0: expected 150 messages, got %d", p0.receivedMessageCount)
	}
	if p0.compressedBytes != 750 {
		t.Errorf("partition 0: expected 750 compressed bytes, got %d", p0.compressedBytes)
	}
	if p0.uncompressedBytes != 1500 {
		t.Errorf("partition 0: expected 1500 uncompressed bytes, got %d", p0.uncompressedBytes)
	}
	if p0.compression != "snappy" {
		t.Errorf("partition 0: expected compression 'snappy', got %q", p0.compression)
	}

	p1 := bc.partitions[1]
	if p1.receivedMessageCount != 30 {
		t.Errorf("partition 1: expected 30 messages, got %d", p1.receivedMessageCount)
	}
	if p1.compression != "lz4" {
		t.Errorf("partition 1: expected compression 'lz4', got %q", p1.compression)
	}
}

func TestBandwidthCollector_BrokerMetadata(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")

	rack := "eu-west-2a"
	meta := kgo.BrokerMetadata{NodeID: 42, Host: "kafka-42.internal", Port: 9093, Rack: &rack}

	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{NumRecords: 1, UncompressedBytes: 10})

	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(bc.brokers))
	}

	b := bc.brokers[42]
	if b.ID != "42" {
		t.Errorf("expected Id '42', got %q", b.ID)
	}
	if b.Host != "kafka-42.internal" {
		t.Errorf("expected Host 'kafka-42.internal', got %q", b.Host)
	}
	if b.Port != "9093" {
		t.Errorf("expected Port '9093', got %q", b.Port)
	}
	if b.Rack != "eu-west-2a" {
		t.Errorf("expected Rack 'eu-west-2a', got %q", b.Rack)
	}
}

func TestBandwidthCollector_NilRack(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")

	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092, Rack: nil}
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{NumRecords: 1, UncompressedBytes: 10})

	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.brokers[1].Rack != "" {
		t.Errorf("nil rack should map to empty string, got %q", bc.brokers[1].Rack)
	}
}

func TestBandwidthCollector_EmitAndReset(t *testing.T) {
	bc := newBandwidthCollector(50*time.Millisecond, "test-group", "test-topic")

	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	bc.callback = func(m nexus.BandwidthMetrics) {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
	}

	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092}
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{
		NumRecords: 100, UncompressedBytes: 1000, CompressedBytes: 500, CompressionType: 1,
	})

	bc.start()
	time.Sleep(150 * time.Millisecond)
	bc.stop()

	mu.Lock()
	defer mu.Unlock()

	if len(received) == 0 {
		t.Fatal("expected at least 1 emitted packet")
	}

	first := received[0]
	if first.TopicName != "test-topic" {
		t.Errorf("expected TopicName 'test-topic', got %q", first.TopicName)
	}
	if first.ConsumerGroup != "test-group" {
		t.Errorf("expected ConsumerGroup 'test-group', got %q", first.ConsumerGroup)
	}
	if first.BandwidthMetricsID == "" {
		t.Error("expected non-empty BandwidthMetricsID")
	}
	if len(first.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(first.Partitions))
	}
	if first.Partitions[0].ReceivedMessageCount != 100 {
		t.Errorf("expected 100 messages, got %d", first.Partitions[0].ReceivedMessageCount)
	}

	// verify accumulators were reset
	bc.mu.Lock()
	acc := bc.partitions[0]
	bc.mu.Unlock()
	if acc.receivedMessageCount != 0 {
		t.Errorf("expected accumulator reset to 0, got %d", acc.receivedMessageCount)
	}
}

func TestBandwidthCollector_UUIDUniqueness(t *testing.T) {
	bc := newBandwidthCollector(20*time.Millisecond, "test-group", "test-topic")

	var ids []string
	var mu sync.Mutex

	bc.callback = func(m nexus.BandwidthMetrics) {
		mu.Lock()
		ids = append(ids, m.BandwidthMetricsID)
		mu.Unlock()
	}

	// add some data so emissions produce packets
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092}
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{NumRecords: 1, UncompressedBytes: 10})

	bc.start()
	time.Sleep(100 * time.Millisecond)

	// add more data between emissions
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{NumRecords: 1, UncompressedBytes: 10})
	time.Sleep(100 * time.Millisecond)
	bc.stop()

	mu.Lock()
	defer mu.Unlock()

	if len(ids) < 2 {
		t.Fatalf("expected at least 2 emissions, got %d", len(ids))
	}

	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate UUID: %s", id)
		}
		seen[id] = true
	}
}

func TestBandwidthCollector_NoCallbackNoPanic(t *testing.T) {
	// verify no panic when callback is nil
	bc := newBandwidthCollector(50*time.Millisecond, "test-group", "test-topic")

	// no callback set — emit should not panic
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092}
	bc.recordFetchBatch(meta, 0, kgo.FetchBatchMetrics{NumRecords: 1, UncompressedBytes: 10})
	bc.emit() // should be safe with nil callback
}

func TestBandwidthCollector_DefaultInterval(t *testing.T) {
	bc := newBandwidthCollector(0, "test-group", "test-topic")
	if bc.interval != nexus.DefaultBandwidthInterval {
		t.Errorf("expected default interval %v, got %v", nexus.DefaultBandwidthInterval, bc.interval)
	}
}

func TestBandwidthCollector_ConcurrentHooks(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")

	var emitted []nexus.BandwidthMetrics
	var mu sync.Mutex
	bc.callback = func(m nexus.BandwidthMetrics) {
		mu.Lock()
		emitted = append(emitted, m)
		mu.Unlock()
	}

	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092}

	var wg sync.WaitGroup
	for i := int32(0); i < 100; i++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			bc.recordFetchBatch(meta, partition%4, kgo.FetchBatchMetrics{
				NumRecords: 10, UncompressedBytes: 100, CompressedBytes: 50, CompressionType: 2,
			})
		}(i)
	}
	wg.Wait()

	bc.emit()

	mu.Lock()
	defer mu.Unlock()

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emission, got %d", len(emitted))
	}

	totalMessages := int64(0)
	for _, p := range emitted[0].Partitions {
		totalMessages += p.ReceivedMessageCount
	}
	if totalMessages != 1000 {
		t.Errorf("expected 1000 total messages across partitions, got %d", totalMessages)
	}
}

func TestCompressionName(t *testing.T) {
	tests := []struct {
		code uint8
		want string
	}{
		{0, "none"},
		{1, "gzip"},
		{2, "snappy"},
		{3, "lz4"},
		{4, "zstd"},
		{5, "unknown(5)"},
		{255, "unknown(255)"},
	}
	for _, tt := range tests {
		if got := compressionName(tt.code); got != tt.want {
			t.Errorf("compressionName(%d) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

func TestGenerateUUID(t *testing.T) {
	uuid := generateUUID()
	if len(uuid) != 36 {
		t.Errorf("expected UUID length 36, got %d: %q", len(uuid), uuid)
	}
	// verify version 4 marker at position 14
	if uuid[14] != '4' {
		t.Errorf("expected version '4' at position 14, got %c", uuid[14])
	}
	// verify variant at position 19 (8, 9, a, b)
	variant := uuid[19]
	if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
		t.Errorf("expected variant [89ab] at position 19, got %c", variant)
	}
}

func TestAdapter_WithBandwidthInterval(t *testing.T) {
	t.Run("valid interval", func(t *testing.T) {
		a := New(context.TODO(), "group", "broker:9092")
		a.WithBandwidthInterval(30 * time.Second)

		if a.bwCollector == nil {
			t.Fatal("expected bwCollector to be created")
		}
		if a.bwCollector.interval != 30*time.Second {
			t.Errorf("expected interval 30s, got %v", a.bwCollector.interval)
		}
	})

	t.Run("zero uses default", func(t *testing.T) {
		a := New(context.TODO(), "group", "broker:9092")
		a.WithBandwidthInterval(0)

		if a.bwCollector.interval != nexus.DefaultBandwidthInterval {
			t.Errorf("expected default interval, got %v", a.bwCollector.interval)
		}
	})

	t.Run("invalid interval panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid interval")
			}
		}()
		a := New(context.TODO(), "group", "broker:9092")
		a.WithBandwidthInterval(-time.Second)
	})
}

func TestAdapter_StatsInterval(t *testing.T) {
	t.Run("with collector", func(t *testing.T) {
		a := New(context.TODO(), "group", "broker:9092")
		a.WithBandwidthInterval(5 * time.Minute)

		if a.StatsInterval() != 5*time.Minute {
			t.Errorf("expected 5m, got %v", a.StatsInterval())
		}
	})

	t.Run("without collector returns default", func(t *testing.T) {
		a := New(context.TODO(), "group", "broker:9092")
		if a.StatsInterval() != nexus.DefaultBandwidthInterval {
			t.Errorf("expected default, got %v", a.StatsInterval())
		}
	})
}

func TestAdapter_SetBandwidthCallback_NilCollector(t *testing.T) {
	// should not panic when no bandwidth collector configured
	a := New(context.TODO(), "group", "broker:9092")
	a.SetBandwidthCallback(func(nexus.BandwidthMetrics) {})
}

func TestAdapter_HookNoOp_NilCollector(t *testing.T) {
	// hooks should not panic when no bandwidth collector configured
	a := New(context.TODO(), "group", "broker:9092")
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker", Port: 9092}

	a.OnFetchBatchRead(meta, "topic", 0, kgo.FetchBatchMetrics{})
	a.OnBrokerRead(meta, 0, 100, 0, 0, nil)
	a.OnBrokerWrite(meta, 0, 100, 0, 0, nil)
}

// TestBandwidthCollector_RecordBrokerRead exercises recordBrokerRead directly
// (the franz-go HookBrokerRead callback path). The body just delegates to
// updateBroker, so a successful call should leave the broker metadata in the
// collector's map.
func TestBandwidthCollector_RecordBrokerRead(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")
	rack := "az-a"
	meta := kgo.BrokerMetadata{NodeID: 7, Host: "broker-7", Port: 9092, Rack: &rack}

	bc.recordBrokerRead(meta, 4096)

	info, ok := bc.brokers[7]
	if !ok {
		t.Fatal("broker 7 should be tracked after recordBrokerRead")
	}
	if info.Host != "broker-7" || info.Rack != "az-a" {
		t.Errorf("broker info = %+v, want host=broker-7 rack=az-a", info)
	}
}

// TestBandwidthCollector_RecordBrokerWrite is the mirror of the read test
// for HookBrokerWrite. The collector currently only records broker metadata
// (TX byte attribution is intentionally not done here - see code comment).
func TestBandwidthCollector_RecordBrokerWrite(t *testing.T) {
	bc := newBandwidthCollector(time.Minute, "test-group", "test-topic")
	meta := kgo.BrokerMetadata{NodeID: 11, Host: "broker-11", Port: 9092}

	bc.recordBrokerWrite(meta, 2048)

	info, ok := bc.brokers[11]
	if !ok {
		t.Fatal("broker 11 should be tracked after recordBrokerWrite")
	}
	if info.Host != "broker-11" {
		t.Errorf("broker info host = %q, want broker-11", info.Host)
	}
}

// TestAdapter_Hooks_WithCollector covers the if-collector-not-nil branch on
// OnFetchBatchRead, OnBrokerRead, OnBrokerWrite (existing test covers the
// nil-collector branch). After invocation we verify the collector's state has
// the relevant updates - confirming the hook delegated to the collector.
func TestAdapter_Hooks_WithCollector(t *testing.T) {
	a := New(context.TODO(), "group", "broker:9092")
	a.WithBandwidthInterval(time.Minute)
	if a.bwCollector == nil {
		t.Fatal("WithBandwidthInterval should install a collector")
	}

	meta := kgo.BrokerMetadata{NodeID: 3, Host: "broker-3", Port: 9092}

	a.OnFetchBatchRead(meta, "topic", 0, kgo.FetchBatchMetrics{
		NumRecords:        10,
		CompressedBytes:   500,
		UncompressedBytes: 1000,
	})
	a.OnBrokerRead(meta, 0, 100, 0, 0, nil)
	a.OnBrokerWrite(meta, 0, 100, 0, 0, nil)

	if _, ok := a.bwCollector.brokers[3]; !ok {
		t.Error("broker 3 should be tracked after hook invocations")
	}
	partAcc, ok := a.bwCollector.partitions[0]
	if !ok {
		t.Fatal("partition 0 should be tracked after OnFetchBatchRead")
	}
	if partAcc.receivedMessageCount != 10 {
		t.Errorf("partition 0 messageCount = %d, want 10", partAcc.receivedMessageCount)
	}
}

// TestAdapter_SetBandwidthCallback_WithCollector covers the non-nil-collector
// path of SetBandwidthCallback (existing test covers the nil-collector early
// return). After registration the callback should be attached and the emit
// loop should be running.
func TestAdapter_SetBandwidthCallback_WithCollector(t *testing.T) {
	a := New(context.TODO(), "group", "broker:9092")
	a.WithBandwidthInterval(time.Minute)
	if a.bwCollector == nil {
		t.Fatal("WithBandwidthInterval should install a collector")
	}

	var mu sync.Mutex
	var receivedCallback bool
	cb := func(nexus.BandwidthMetrics) {
		mu.Lock()
		receivedCallback = true
		mu.Unlock()
	}

	a.SetBandwidthCallback(cb)

	// The callback function reference itself should now be stored. Compare via
	// invoking it - reflect.Value comparison on funcs is unreliable.
	if a.bwCollector.callback == nil {
		t.Error("callback should be registered on collector")
	}
	// invoke the registered callback directly to validate it's our cb
	a.bwCollector.callback(nexus.BandwidthMetrics{})
	mu.Lock()
	got := receivedCallback
	mu.Unlock()
	if !got {
		t.Error("registered callback was not the one we passed in")
	}

	// stop the emit loop to avoid goroutine leak in tests
	a.bwCollector.stop()
}
