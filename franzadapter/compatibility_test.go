// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCompatibilityBalancers_ReturnsFourBalancers(t *testing.T) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("test-topic"),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		CompatibilityBalancers(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	val := client.OptValue(kgo.Balancers)
	if val == nil {
		t.Fatal("expected balancers to be configured")
	}

	balancers, ok := val.([]kgo.GroupBalancer)
	if !ok {
		t.Fatalf("expected []kgo.GroupBalancer, got %T", val)
	}

	if len(balancers) != 4 {
		t.Errorf("expected 4 balancers, got %d", len(balancers))
	}

	// verify order: cooperative-sticky, sticky, roundrobin, range
	expectedNames := []string{"cooperative-sticky", "sticky", "roundrobin", "range"}
	for i, b := range balancers {
		if b.ProtocolName() != expectedNames[i] {
			t.Errorf("balancer[%d] = %s, want %s", i, b.ProtocolName(), expectedNames[i])
		}
	}

	// verify first is cooperative, rest are eager
	if !balancers[0].IsCooperative() {
		t.Error("first balancer should be cooperative")
	}
	for i := 1; i < len(balancers); i++ {
		if balancers[i].IsCooperative() {
			t.Errorf("balancer[%d] should be eager, not cooperative", i)
		}
	}
}

func TestLoggingBalancer_PreferredBalancer_LogsInfo(t *testing.T) {
	// create a logging balancer at index 0 (preferred)
	lb := &loggingBalancer{
		GroupBalancer: kgo.CooperativeStickyBalancer(),
		index:         0,
		skipped:       nil,
	}

	output := captureLogOutput(func() {
		// ParseSyncAssignment triggers the logging
		// empty assignment is fine - we just want to trigger the log
		_, _ = lb.ParseSyncAssignment([]byte{})
	})

	if !strings.Contains(output, "cooperative-sticky") {
		t.Errorf("should log balancer name, got: %s", output)
	}
	if !strings.Contains(output, "cooperative") {
		t.Errorf("should log protocol type, got: %s", output)
	}
	if strings.Contains(output, "WARNING") {
		t.Errorf("preferred balancer should not warn, got: %s", output)
	}
	if strings.Contains(output, "fallback") {
		t.Errorf("preferred balancer should not mention fallback, got: %s", output)
	}
}

func TestLoggingBalancer_FallbackBalancer_LogsWarning(t *testing.T) {
	// create a logging balancer at index 2 (second fallback)
	lb := &loggingBalancer{
		GroupBalancer: kgo.RoundRobinBalancer(),
		index:         2,
		skipped:       []string{"cooperative-sticky", "sticky"},
	}

	output := captureLogOutput(func() {
		_, _ = lb.ParseSyncAssignment([]byte{})
	})

	if !strings.Contains(output, "WARNING") {
		t.Errorf("fallback balancer should warn, got: %s", output)
	}
	if !strings.Contains(output, "roundrobin") {
		t.Errorf("should log balancer name, got: %s", output)
	}
	if !strings.Contains(output, "fallback") {
		t.Errorf("should mention fallback, got: %s", output)
	}
	if !strings.Contains(output, "cooperative-sticky") {
		t.Errorf("should list skipped balancers, got: %s", output)
	}
	if !strings.Contains(output, "sticky") {
		t.Errorf("should list skipped balancers, got: %s", output)
	}
	if !strings.Contains(output, "not supported by all group members") {
		t.Errorf("should explain why fallback was used, got: %s", output)
	}
}

func TestLoggingBalancer_LogsOnlyOnce(t *testing.T) {
	lb := &loggingBalancer{
		GroupBalancer: kgo.CooperativeStickyBalancer(),
		index:         0,
		skipped:       nil,
	}

	// call ParseSyncAssignment multiple times
	callCount := 0
	output := captureLogOutput(func() {
		for i := 0; i < 5; i++ {
			_, _ = lb.ParseSyncAssignment([]byte{})
			callCount++
		}
	})

	// should only log once despite multiple calls
	logLines := strings.Split(strings.TrimSpace(output), "\n")
	nonEmptyLines := 0
	for _, line := range logLines {
		if strings.TrimSpace(line) != "" {
			nonEmptyLines++
		}
	}

	if nonEmptyLines != 1 {
		t.Errorf("expected 1 log line, got %d: %s", nonEmptyLines, output)
	}
}

func TestLoggingBalancer_DelegatesToWrapped(t *testing.T) {
	wrapped := kgo.RangeBalancer()
	lb := &loggingBalancer{
		GroupBalancer: wrapped,
		index:         3,
		skipped:       []string{"cooperative-sticky", "sticky", "roundrobin"},
	}

	// verify delegation
	if lb.ProtocolName() != wrapped.ProtocolName() {
		t.Errorf("ProtocolName not delegated: got %s, want %s",
			lb.ProtocolName(), wrapped.ProtocolName())
	}

	if lb.IsCooperative() != wrapped.IsCooperative() {
		t.Errorf("IsCooperative not delegated: got %v, want %v",
			lb.IsCooperative(), wrapped.IsCooperative())
	}
}

func TestLoggingBalancer_EagerFallback_LogsEagerProtocol(t *testing.T) {
	lb := &loggingBalancer{
		GroupBalancer: kgo.StickyBalancer(),
		index:         1,
		skipped:       []string{"cooperative-sticky"},
	}

	output := captureLogOutput(func() {
		_, _ = lb.ParseSyncAssignment([]byte{})
	})

	if !strings.Contains(output, "(eager)") {
		t.Errorf("should log eager protocol type, got: %s", output)
	}
}

// captureLogOutput redirects log output and returns it as a string
func captureLogOutput(fn func()) string {
	var buf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(originalOutput)

	fn()

	return buf.String()
}

// TestLoggingBalancer_ParseSyncAssignment_PropagatesError covers the error
// branch where the wrapped balancer's ParseSyncAssignment fails. Existing
// tests pass an empty byte slice which parses as a zero-length assignment
// successfully. Garbage bytes that don't match the Kafka GroupAssignment
// wire format trigger the underlying decoder to return an error.
func TestLoggingBalancer_ParseSyncAssignment_PropagatesError(t *testing.T) {
	lb := &loggingBalancer{
		GroupBalancer: kgo.RangeBalancer(),
		index:         0,
	}
	// Malformed wire bytes - way too short to be a valid GroupAssignment
	// (which begins with a 2-byte version, then arrays with 4-byte length
	// prefixes). A single byte can't parse to anything valid.
	_, err := lb.ParseSyncAssignment([]byte{0xff})
	if err == nil {
		t.Fatal("expected ParseSyncAssignment to error on malformed bytes")
	}
	if !strings.Contains(err.Error(), "parse sync assignment") {
		t.Errorf("error should be wrapped with 'parse sync assignment': %v", err)
	}
}
