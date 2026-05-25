// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testTopicName = "test-topic"

// --- Helper functions to create mock Fetches ---

// makeFetches creates a Fetches with the given records.
func makeFetches(records ...*kgo.Record) kgo.Fetches {
	if len(records) == 0 {
		return kgo.Fetches{}
	}

	partitions := make(map[int32][]*kgo.Record)
	for _, r := range records {
		partitions[r.Partition] = append(partitions[r.Partition], r)
	}

	var fetchPartitions []kgo.FetchPartition
	for partition, recs := range partitions {
		fetchPartitions = append(fetchPartitions, kgo.FetchPartition{
			Partition: partition,
			Records:   recs,
		})
	}

	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      testTopicName,
			Partitions: fetchPartitions,
		}},
	}}
}

// makeFetchesWithError creates a Fetches with a partition error.
func makeFetchesWithError(partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: testTopicName,
			Partitions: []kgo.FetchPartition{{
				Partition: partition,
				Err:       err,
			}},
		}},
	}}
}

func TestExtractEnvelope(t *testing.T) {
	t.Run("UTF-8 string key", func(t *testing.T) {
		adapter := &Adapter{ctx: context.Background()}
		record := &kgo.Record{
			Key:       []byte("user-123"),
			Partition: 3,
			Offset:    42,
		}

		envelope := adapter.ExtractEnvelope(record)

		if envelope.Key != "user-123" {
			t.Errorf("Key = %q, want %q", envelope.Key, "user-123")
		}
		if envelope.Partition != 3 {
			t.Errorf("Partition = %d, want 3", envelope.Partition)
		}
		if envelope.Offset != 42 {
			t.Errorf("Offset = %d, want 42", envelope.Offset)
		}
	})

	t.Run("binary key is base64 encoded", func(t *testing.T) {
		adapter := &Adapter{ctx: context.Background()}
		// invalid UTF-8 sequence
		record := &kgo.Record{
			Key:       []byte{0xFF, 0xFE, 0x00, 0x01},
			Partition: 1,
			Offset:    100,
		}

		envelope := adapter.ExtractEnvelope(record)

		// base64 of [0xFF, 0xFE, 0x00, 0x01] = "//4AAQ=="
		expected := "//4AAQ=="
		if envelope.Key != expected {
			t.Errorf("Key = %q, want %q (base64 of binary)", envelope.Key, expected)
		}
	})

	t.Run("empty key falls back to partition number", func(t *testing.T) {
		adapter := &Adapter{ctx: context.Background()}
		record := &kgo.Record{
			Key:       nil,
			Partition: 7,
			Offset:    200,
		}

		envelope := adapter.ExtractEnvelope(record)

		if envelope.Key != "7" {
			t.Errorf("Key = %q, want %q (partition as string)", envelope.Key, "7")
		}
	})

	t.Run("empty byte slice key falls back to partition", func(t *testing.T) {
		adapter := &Adapter{ctx: context.Background()}
		record := &kgo.Record{
			Key:       []byte{},
			Partition: 5,
			Offset:    300,
		}

		envelope := adapter.ExtractEnvelope(record)

		if envelope.Key != "5" {
			t.Errorf("Key = %q, want %q (partition as string)", envelope.Key, "5")
		}
	})

	t.Run("context is passed through", func(t *testing.T) {
		type testContextKey struct{}
		ctx := context.WithValue(context.Background(), testContextKey{}, "test-value")
		adapter := &Adapter{ctx: ctx}
		record := &kgo.Record{
			Key:       []byte("key"),
			Partition: 0,
			Offset:    0,
		}

		envelope := adapter.ExtractEnvelope(record)

		if envelope.Ctx != ctx {
			t.Error("context should be passed through")
		}
		if envelope.Ctx.Value(testContextKey{}) != "test-value" {
			t.Error("context values should be preserved")
		}
	})

	t.Run("unicode UTF-8 key", func(t *testing.T) {
		adapter := &Adapter{ctx: context.Background()}
		record := &kgo.Record{
			Key:       []byte("日本語キー"),
			Partition: 2,
			Offset:    50,
		}

		envelope := adapter.ExtractEnvelope(record)

		if envelope.Key != "日本語キー" {
			t.Errorf("Key = %q, want %q", envelope.Key, "日本語キー")
		}
	})
}

func TestAckRebalance(t *testing.T) {
	t.Run("is a no-op", func(t *testing.T) {
		adapter := &Adapter{}

		err := adapter.AckRebalance(nexus.Assign, nil)

		if err != nil {
			t.Errorf("AckRebalance should return nil, got %v", err)
		}
	})

	t.Run("accepts any rebalance type", func(t *testing.T) {
		adapter := &Adapter{}

		errAssign := adapter.AckRebalance(nexus.Assign, []nexus.RebalanceInfo{
			{Partition: 0, TopicName: "test"},
		})
		errRevoke := adapter.AckRebalance(nexus.Revoke, []nexus.RebalanceInfo{
			{Partition: 1, TopicName: "test"},
		})

		if errAssign != nil || errRevoke != nil {
			t.Errorf("AckRebalance should return nil for all types")
		}
	})
}

func TestBrokerQuery(t *testing.T) {
	t.Run("returns empty response", func(t *testing.T) {
		adapter := &Adapter{}

		response, err := adapter.BrokerQuery(nexus.QueryRequest{})

		if err != nil {
			t.Errorf("BrokerQuery should return nil error, got %v", err)
		}
		if response.QueryType != 0 {
			t.Errorf("response.QueryType = %d, want 0 (empty)", response.QueryType)
		}
	})
}

func TestSubscribe(t *testing.T) {
	t.Run("logs subscription", func(t *testing.T) {
		logger := &capturingLogger{}
		adapter := &Adapter{
			ctx:       context.Background(),
			logger:    logger,
			topicName: "orders",
		}

		err := adapter.Subscribe()

		if err != nil {
			t.Errorf("Subscribe should return nil, got %v", err)
		}
		if !logger.infoCalled {
			t.Error("Subscribe should log info message")
		}
		// the format string contains "subscribed to topic"
		if !contains(logger.lastInfoMsg, "subscribed to topic") {
			t.Errorf("log message should mention subscription, got %q", logger.lastInfoMsg)
		}
	})
}

func TestUnsubscribe(t *testing.T) {
	t.Run("handles nil closeFn", func(t *testing.T) {
		adapter := &Adapter{closeFn: nil}

		err := adapter.Unsubscribe()

		if err != nil {
			t.Errorf("Unsubscribe should handle nil closeFn, got %v", err)
		}
	})

	t.Run("calls closeFn", func(t *testing.T) {
		closeCalled := false
		adapter := &Adapter{
			closeFn: func() { closeCalled = true },
		}

		err := adapter.Unsubscribe()

		if err != nil {
			t.Errorf("Unsubscribe returned error: %v", err)
		}
		if !closeCalled {
			t.Error("expected closeFn to be called")
		}
	})
}

func TestPoll_NilClient(t *testing.T) {
	t.Run("returns error when client is nil", func(t *testing.T) {
		adapter := &Adapter{client: nil}

		_, _, err := adapter.Poll(100)

		if err == nil {
			t.Error("Poll should return error when client is nil")
		}
		if !contains(err.Error(), "client closed") {
			t.Errorf("error = %q, want to contain 'client closed'", err.Error())
		}
	})
}

// --- Poll Tests with Mocks ---

func TestPoll_ReturnsMessage(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	expectedRecord := &kgo.Record{
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Partition: 3,
		Offset:    42,
	}

	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return makeFetches(expectedRecord)
	}
	adapter.allowRebalanceFn = func() {}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if msg != expectedRecord {
		t.Error("expected message to be returned")
	}
}

func TestPoll_EmptyFetches(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return kgo.Fetches{}
	}
	adapter.allowRebalanceFn = func() {}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for empty fetches")
	}
	if msg != nil {
		t.Error("expected nil message for empty fetches")
	}
}

func TestPoll_PartitionError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	expectedErr := errors.New("partition error")
	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return makeFetchesWithError(0, expectedErr)
	}
	adapter.allowRebalanceFn = func() {}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
	if ok {
		t.Error("expected ok=false")
	}
	if msg != nil {
		t.Error("expected nil message")
	}
}

func TestPoll_IgnoresTimeoutError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	// context.DeadlineExceeded is ignored
	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return makeFetchesWithError(0, context.DeadlineExceeded)
	}
	adapter.allowRebalanceFn = func() {}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err != nil {
		t.Errorf("timeout error should be ignored, got: %v", err)
	}
	if ok {
		t.Error("expected ok=false")
	}
	if msg != nil {
		t.Error("expected nil message")
	}
}

func TestPoll_ClientClosed(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	// franz-go returns a special fetch when client is closed
	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return kgo.NewErrFetch(kgo.ErrClientClosed)
	}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err == nil {
		t.Error("expected error for closed client")
	}
	if !contains(err.Error(), "client closed") {
		t.Errorf("expected 'client closed' error, got: %v", err)
	}
	if ok {
		t.Error("expected ok=false")
	}
	if msg != nil {
		t.Error("expected nil message")
	}
}

func TestPoll_CallsAllowRebalance(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	allowRebalanceCalled := false
	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return kgo.Fetches{}
	}
	adapter.allowRebalanceFn = func() {
		allowRebalanceCalled = true
	}

	_, _, _ = adapter.Poll(100 * time.Millisecond)

	if !allowRebalanceCalled {
		t.Error("expected allowRebalanceFn to be called")
	}
}

func TestPoll_ProcessesPendingRebalances(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = map[string][]int32{testTopicName: {0, 1, 2}}
	adapter.pendingRevoked = make(map[string][]int32)

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return kgo.Fetches{}
	}
	adapter.allowRebalanceFn = func() {}

	_, _, err := adapter.Poll(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called for pending assigns")
	}
}

func TestPoll_MultipleRecords_ReturnsFirst(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}
	adapter.pendingAssigned = make(map[string][]int32)
	adapter.pendingRevoked = make(map[string][]int32)

	firstRecord := &kgo.Record{
		Key:       []byte("first"),
		Partition: 0,
		Offset:    0,
	}
	secondRecord := &kgo.Record{
		Key:       []byte("second"),
		Partition: 0,
		Offset:    1,
	}

	adapter.pollRecordsFn = func(_ context.Context, _ int) kgo.Fetches {
		return makeFetches(firstRecord, secondRecord)
	}
	adapter.allowRebalanceFn = func() {}

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if msg != firstRecord {
		t.Error("expected first record to be returned")
	}
}

// --- CommitOffsets Tests ---

func TestCommitOffsets_EmptyMessages(t *testing.T) {
	adapter := NewCustom()

	failed, err := adapter.CommitOffsets(nil)

	if failed != nil {
		t.Error("expected nil failed messages")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCommitOffsets_Success(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}

	var committedRecords []*kgo.Record
	adapter.commitRecordsFn = func(_ context.Context, rs ...*kgo.Record) error {
		committedRecords = rs
		return nil
	}

	record := &kgo.Record{
		Key:       []byte("key"),
		Partition: 2,
		Offset:    99,
	}
	msg := &nexus.Message[*kgo.Record]{
		Partition: 2,
		Offset:    99,
		Payload:   &record,
	}

	failed, err := adapter.CommitOffsets([]*nexus.Message[*kgo.Record]{msg})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Error("expected nil failed messages on success")
	}
	if len(committedRecords) != 1 {
		t.Fatalf("expected 1 committed record, got %d", len(committedRecords))
	}
	if committedRecords[0] != record {
		t.Error("expected the same record to be committed")
	}
}

func TestCommitOffsets_MultipleMessages(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}

	var committedRecords []*kgo.Record
	adapter.commitRecordsFn = func(_ context.Context, rs ...*kgo.Record) error {
		committedRecords = rs
		return nil
	}

	messages := make([]*nexus.Message[*kgo.Record], 3)
	for i := range 3 {
		record := &kgo.Record{
			Key:       []byte("key"),
			Partition: int32(i), //nolint:gosec // bounded test loop
			Offset:    int64(i * 100),
		}
		messages[i] = &nexus.Message[*kgo.Record]{
			Partition: int32(i), //nolint:gosec // bounded test loop
			Offset:    int64(i * 100),
			Payload:   &record,
		}
	}

	failed, err := adapter.CommitOffsets(messages)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Error("expected nil failed messages on success")
	}
	if len(committedRecords) != 3 {
		t.Fatalf("expected 3 committed records, got %d", len(committedRecords))
	}
}

func TestCommitOffsets_Error(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &capturingLogger{}

	expectedErr := errors.New("commit failed")
	adapter.commitRecordsFn = func(_ context.Context, _ ...*kgo.Record) error {
		return expectedErr
	}

	record := &kgo.Record{
		Partition: 0,
		Offset:    0,
	}
	msg := &nexus.Message[*kgo.Record]{
		Payload: &record,
	}

	failed, err := adapter.CommitOffsets([]*nexus.Message[*kgo.Record]{msg})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if failed != nil {
		t.Error("franz adapter returns nil on error (unlike kafka adapter)")
	}
}

func TestCommitOffsets_LogsOnSuccess(t *testing.T) {
	logger := &capturingLogger{}
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = logger

	adapter.commitRecordsFn = func(_ context.Context, _ ...*kgo.Record) error {
		return nil
	}

	record := &kgo.Record{Partition: 0, Offset: 0}
	msg := &nexus.Message[*kgo.Record]{Payload: &record}

	_, _ = adapter.CommitOffsets([]*nexus.Message[*kgo.Record]{msg})

	if !logger.debugCalled {
		t.Error("expected debug log on successful commit")
	}
}

func TestCommitOffsets_LogsOnError(t *testing.T) {
	logger := &capturingLogger{}
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = logger

	adapter.commitRecordsFn = func(_ context.Context, _ ...*kgo.Record) error {
		return errors.New("commit failed")
	}

	record := &kgo.Record{Partition: 0, Offset: 0}
	msg := &nexus.Message[*kgo.Record]{Payload: &record}

	_, _ = adapter.CommitOffsets([]*nexus.Message[*kgo.Record]{msg})

	if !logger.errorCalled {
		t.Error("expected error log on failed commit")
	}
}

// --- Unsubscribe Tests with Mocks ---

func TestUnsubscribe_CallsCloseFn(t *testing.T) {
	adapter := NewCustom()

	closeCalled := false
	adapter.closeFn = func() {
		closeCalled = true
	}

	err := adapter.Unsubscribe()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !closeCalled {
		t.Error("expected closeFn to be called")
	}
}

func TestUnsubscribe_NilCloseFn(t *testing.T) {
	adapter := NewCustom()
	adapter.closeFn = nil

	err := adapter.Unsubscribe()

	if err != nil {
		t.Errorf("expected nil error for nil closeFn, got: %v", err)
	}
}

// --- Interface Compliance ---

func TestAdapter_ImplementsBrokerPort(_ *testing.T) {
	var _ nexus.BrokerPort[*kgo.Record] = (*Adapter)(nil)
}

// capturingLogger captures log calls for verification
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

// TestConsumerGroup covers the trivial-but-uncovered ConsumerGroup() getter.
// New()/NewWithOptions stash the groupID; NewCustom leaves it empty.
func TestConsumerGroup(t *testing.T) {
	t.Run("returns groupID from New", func(t *testing.T) {
		a := New(context.Background(), "my-group", "broker:9092")
		if got := a.ConsumerGroup(); got != "my-group" {
			t.Errorf("ConsumerGroup() = %q, want %q", got, "my-group")
		}
	})

	t.Run("returns empty for NewCustom", func(t *testing.T) {
		a := NewCustom()
		if got := a.ConsumerGroup(); got != "" {
			t.Errorf("ConsumerGroup() = %q, want empty string", got)
		}
	})
}

// TestUnsubscribe_WithBandwidthCollector covers the if-bwCollector-not-nil
// branch of Unsubscribe (the existing test path covers nil-collector via
// SetClient on plain NewCustom). The collector's stop() should be invoked,
// shutting down its emit goroutine.
func TestUnsubscribe_WithBandwidthCollector(t *testing.T) {
	a := New(context.Background(), "group", "broker:9092")
	a.WithBandwidthInterval(time.Minute)
	if a.bwCollector == nil {
		t.Fatal("WithBandwidthInterval should install a collector")
	}
	// kick the emit loop into running so stop() actually has work to do
	a.bwCollector.callback = func(nexus.BandwidthMetrics) {}
	a.bwCollector.start()

	if err := a.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
}

