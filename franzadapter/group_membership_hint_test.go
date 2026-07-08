// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordingLogger captures Warn calls so the diagnostic hint can be asserted.
type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (r *recordingLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (r *recordingLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (r *recordingLogger) Error(_ context.Context, _ string, _ ...any) {}
func (r *recordingLogger) Warn(_ context.Context, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warns = append(r.warns, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.warns))
	copy(out, r.warns)
	return out
}

// newClientWithSessionTimeout builds a kgo.Client without contacting a broker.
// kgo.NewClient is lazy - it validates options but doesn't dial until something
// triggers I/O - so this is safe in unit tests. Caller closes via t.Cleanup.
func newClientWithSessionTimeout(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	all := append([]kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("hint-test"),
		kgo.ConsumeTopics("hint-test-topic"),
	}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		t.Fatalf("kgo.NewClient failed: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// --- logGroupMembershipHintIfApplicable ---

func TestLogGroupMembershipHint_FiresOnUnknownMember(t *testing.T) {
	cl := newClientWithSessionTimeout(t, kgo.SessionTimeout(6*time.Second))
	rec := &recordingLogger{}
	a := &Adapter{
		ctx:    context.Background(),
		logger: rec,
		client: cl,
	}

	a.logGroupMembershipHintIfApplicable(kerr.UnknownMemberID)

	warns := rec.snapshot()
	if len(warns) != 1 {
		t.Fatalf("expected one Warn log, got %d: %v", len(warns), warns)
	}
	if !strings.Contains(warns[0], "group membership lost") {
		t.Errorf("warn missing 'group membership lost' phrase: %q", warns[0])
	}
	if !strings.Contains(warns[0], "6s") {
		t.Errorf("warn should surface configured session timeout (6s), got: %q", warns[0])
	}
	if !strings.Contains(warns[0], "kgo.SessionTimeout") {
		t.Errorf("warn should reference the franz-go option to raise: %q", warns[0])
	}
}

func TestLogGroupMembershipHint_FiresOnIllegalGeneration(t *testing.T) {
	cl := newClientWithSessionTimeout(t)
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec, client: cl}

	a.logGroupMembershipHintIfApplicable(kerr.IllegalGeneration)

	if got := len(rec.snapshot()); got != 1 {
		t.Fatalf("expected one Warn log, got %d", got)
	}
}

func TestLogGroupMembershipHint_FiresOnWrappedError(t *testing.T) {
	// kerr errors should be matchable through errors.Is wrapping.
	cl := newClientWithSessionTimeout(t)
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec, client: cl}

	wrapped := fmt.Errorf("commit failed: %w", kerr.UnknownMemberID)
	a.logGroupMembershipHintIfApplicable(wrapped)

	if got := len(rec.snapshot()); got != 1 {
		t.Fatalf("expected wrapped kerr to be detected via errors.Is, got %d Warns", got)
	}
}

func TestLogGroupMembershipHint_SilentOnUnrelatedError(t *testing.T) {
	cl := newClientWithSessionTimeout(t)
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec, client: cl}

	a.logGroupMembershipHintIfApplicable(errors.New("network unreachable"))
	a.logGroupMembershipHintIfApplicable(kerr.RequestTimedOut)
	a.logGroupMembershipHintIfApplicable(kerr.RebalanceInProgress)

	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("expected zero Warn logs for unrelated errors, got %d: %v", got, rec.snapshot())
	}
}

func TestLogGroupMembershipHint_NilClientNoPanic(t *testing.T) {
	// Defensive: if hint fires before client is wired up, fall back gracefully
	// rather than nil-panicking. Should still log, just with a placeholder.
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec, client: nil}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil client caused panic: %v", r)
		}
	}()

	a.logGroupMembershipHintIfApplicable(kerr.UnknownMemberID)

	warns := rec.snapshot()
	if len(warns) != 1 {
		t.Fatalf("expected one Warn even with nil client, got %d", len(warns))
	}
	if !strings.Contains(warns[0], "client not initialised") {
		t.Errorf("warn should indicate uninitialised client: %q", warns[0])
	}
}

// --- currentSessionTimeoutDescription ---

func TestCurrentSessionTimeoutDescription_NilClient(t *testing.T) {
	a := &Adapter{client: nil}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "client not initialised") {
		t.Fatalf("expected description to flag missing client, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_DefaultSessionTimeout(t *testing.T) {
	// franz-go's internal SessionTimeout default is 45s. OptValue should return
	// it even when the caller didn't override.
	cl := newClientWithSessionTimeout(t)
	a := &Adapter{client: cl}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "45s") {
		t.Fatalf("expected franz-go default 45s, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_ExplicitOverride(t *testing.T) {
	cl := newClientWithSessionTimeout(t, kgo.SessionTimeout(10*time.Second))
	a := &Adapter{client: cl}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "10s") {
		t.Fatalf("expected explicit override 10s, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_SubSecondOverride(t *testing.T) {
	// Edge case: very small session timeout; the description function should
	// still report it correctly. franz-go >= 1.21.5 validates heartbeat <
	// session timeout at construction, so pair the override with a smaller
	// heartbeat to keep the client constructible.
	cl := newClientWithSessionTimeout(t, kgo.SessionTimeout(500*time.Millisecond),
		kgo.HeartbeatInterval(100*time.Millisecond))
	a := &Adapter{client: cl}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "500ms") {
		t.Fatalf("expected sub-second value 500ms, got %q", got)
	}
}

// --- CommitOffsets integration: hint surfaces alongside the Error log ---

func TestCommitOffsets_UnknownMember_LogsDiagnosticHint(t *testing.T) {
	cl := newClientWithSessionTimeout(t, kgo.SessionTimeout(6*time.Second))
	rec := &recordingLogger{}
	a := &Adapter{
		ctx:    context.Background(),
		logger: rec,
		client: cl,
		commitRecordsFn: func(_ context.Context, _ ...*kgo.Record) error {
			return kerr.UnknownMemberID
		},
	}

	rec_msg := &nexus.Message[*kgo.Record]{
		Payload: func() **kgo.Record {
			r := &kgo.Record{Topic: "t", Partition: 0, Offset: 0}
			return &r
		}(),
	}

	_, err := a.CommitOffsets([]*nexus.Message[*kgo.Record]{rec_msg})
	if err == nil {
		t.Fatalf("expected commit to surface the failure, got nil")
	}

	warns := rec.snapshot()
	if len(warns) != 1 {
		t.Fatalf("expected one diagnostic Warn, got %d: %v", len(warns), warns)
	}
	if !strings.Contains(warns[0], "group membership lost") {
		t.Errorf("Warn should describe failure mode: %q", warns[0])
	}
	if !strings.Contains(warns[0], "6s") {
		t.Errorf("Warn should surface configured session timeout: %q", warns[0])
	}
}

func TestCommitOffsets_GenericFailure_NoHint(t *testing.T) {
	// Non-membership commit failures should still log Error but not the hint.
	cl := newClientWithSessionTimeout(t)
	rec := &recordingLogger{}
	a := &Adapter{
		ctx:    context.Background(),
		logger: rec,
		client: cl,
		commitRecordsFn: func(_ context.Context, _ ...*kgo.Record) error {
			return errors.New("generic commit failure")
		},
	}

	rec_msg := &nexus.Message[*kgo.Record]{
		Payload: func() **kgo.Record {
			r := &kgo.Record{Topic: "t", Partition: 0, Offset: 0}
			return &r
		}(),
	}
	_, _ = a.CommitOffsets([]*nexus.Message[*kgo.Record]{rec_msg})

	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("generic failure must not produce diagnostic Warn, got %d: %v", got, rec.snapshot())
	}
}
