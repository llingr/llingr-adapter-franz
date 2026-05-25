// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClient_ValidConfig(t *testing.T) {
	t.Run("passes with all required options", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		// should not panic
		Client(client)
	})
}

func TestEnsureAutoCommitDisabled(t *testing.T) {
	t.Run("panics when auto-commit not disabled", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			// DisableAutoCommit() not set
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic when auto-commit not disabled")
			}
			msg, ok := r.(string)
			if !ok {
				t.Errorf("panic value not string: %v", r)
			}
			if !strings.Contains(msg, "DisableAutoCommit") {
				t.Errorf("panic message should mention DisableAutoCommit: %s", msg)
			}
		}()

		ensureAutoCommitDisabled(client)
	})

	t.Run("does not panic when auto-commit disabled", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		// should not panic
		ensureAutoCommitDisabled(client)
	})
}

func TestEnsureBlockRebalanceOnPoll(t *testing.T) {
	t.Run("panics when BlockRebalanceOnPoll not set", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			// BlockRebalanceOnPoll() not set
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic when BlockRebalanceOnPoll not set")
			}
			msg, ok := r.(string)
			if !ok {
				t.Errorf("panic value not string: %v", r)
			}
			if !strings.Contains(msg, "BlockRebalanceOnPoll") {
				t.Errorf("panic message should mention BlockRebalanceOnPoll: %s", msg)
			}
		}()

		ensureBlockRebalanceOnPoll(client)
	})

	t.Run("does not panic when BlockRebalanceOnPoll set", func(t *testing.T) {
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

		// should not panic
		ensureBlockRebalanceOnPoll(client)
	})
}

func TestLogBalancerProtocol(t *testing.T) {
	t.Run("logs cooperative for CooperativeStickyBalancer", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.Balancers(kgo.CooperativeStickyBalancer()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		if !strings.Contains(output, "cooperative") {
			t.Errorf("should log cooperative protocol, got: %s", output)
		}
		if !strings.Contains(output, "cooperative-sticky") {
			t.Errorf("should log balancer name, got: %s", output)
		}
		if !strings.Contains(output, "only affected partitions") {
			t.Errorf("should describe cooperative behaviour, got: %s", output)
		}
	})

	t.Run("logs eager for StickyBalancer", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.Balancers(kgo.StickyBalancer()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		if !strings.Contains(output, "eager") {
			t.Errorf("should log eager protocol, got: %s", output)
		}
		if !strings.Contains(output, "sticky") {
			t.Errorf("should log balancer name, got: %s", output)
		}
		if !strings.Contains(output, "all partitions revoked") {
			t.Errorf("should describe eager behaviour, got: %s", output)
		}
	})

	t.Run("logs eager for RangeBalancer", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.Balancers(kgo.RangeBalancer()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		if !strings.Contains(output, "eager") {
			t.Errorf("should log eager protocol, got: %s", output)
		}
		if !strings.Contains(output, "range") {
			t.Errorf("should log balancer name, got: %s", output)
		}
	})

	t.Run("logs eager for RoundRobinBalancer", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.Balancers(kgo.RoundRobinBalancer()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		if !strings.Contains(output, "eager") {
			t.Errorf("should log eager protocol, got: %s", output)
		}
		if !strings.Contains(output, "roundrobin") {
			t.Errorf("should log balancer name, got: %s", output)
		}
	})

	t.Run("logs default CooperativeStickyBalancer when not explicitly configured", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			// No explicit Balancers - franz-go defaults to CooperativeStickyBalancer
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		// Franz-go defaults to cooperative-sticky, which should be logged
		if !strings.Contains(output, "cooperative") {
			t.Errorf("should log default cooperative balancer, got: %s", output)
		}
	})

	t.Run("skips logging when multiple balancers configured", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			// Multiple balancers - e.g., CompatibilityBalancers() case
			// The loggingBalancer wrapper will handle logging the selected protocol
			kgo.Balancers(kgo.CooperativeStickyBalancer(), kgo.RangeBalancer()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			logBalancerProtocol(client)
		})

		// Should NOT log - loggingBalancer handles this case
		if output != "" {
			t.Errorf("should not log when multiple balancers configured, got: %s", output)
		}
	})
}

func TestWarnIfReadUncommitted(t *testing.T) {
	t.Run("warns when ReadUncommitted (default)", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			// FetchIsolationLevel not set - defaults to ReadUncommitted
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfReadUncommitted(client)
		})

		if !strings.Contains(output, "ReadUncommitted") {
			t.Errorf("should warn about ReadUncommitted, got: %s", output)
		}
	})

	t.Run("warns when explicitly set to ReadUncommitted", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfReadUncommitted(client)
		})

		if !strings.Contains(output, "ReadUncommitted") {
			t.Errorf("should warn about ReadUncommitted, got: %s", output)
		}
	})

	t.Run("does not warn when ReadCommitted", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfReadUncommitted(client)
		})

		if strings.Contains(output, "ReadUncommitted") {
			t.Errorf("should not warn when ReadCommitted: %s", output)
		}
	})
}

func TestWarnIfSessionTimeoutTooLow(t *testing.T) {
	t.Run("warns when below 25 seconds", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.SessionTimeout(10*time.Second),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfSessionTimeoutTooLow(client)
		})

		if !strings.Contains(output, "SessionTimeout") {
			t.Errorf("should warn about low SessionTimeout, got: %s", output)
		}
	})

	t.Run("does not warn when at or above 25 seconds", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.SessionTimeout(30*time.Second),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfSessionTimeoutTooLow(client)
		})

		if strings.Contains(output, "SessionTimeout") {
			t.Errorf("should not warn when SessionTimeout >= 25s: %s", output)
		}
	})

	t.Run("does not warn when using default", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			// SessionTimeout not set - uses default (45s)
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfSessionTimeoutTooLow(client)
		})

		if strings.Contains(output, "SessionTimeout") {
			t.Errorf("should not warn when using default: %s", output)
		}
	})
}

func TestWarnIfRebalanceTimeoutTooLow(t *testing.T) {
	t.Run("warns when below 30 seconds", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.RebalanceTimeout(15*time.Second),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfRebalanceTimeoutTooLow(client)
		})

		if !strings.Contains(output, "RebalanceTimeout") {
			t.Errorf("should warn about low RebalanceTimeout, got: %s", output)
		}
	})

	t.Run("does not warn when at or above 30 seconds", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.RebalanceTimeout(45*time.Second),
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfRebalanceTimeoutTooLow(client)
		})

		if strings.Contains(output, "RebalanceTimeout") {
			t.Errorf("should not warn when RebalanceTimeout >= 30s: %s", output)
		}
	})

	t.Run("does not warn when using default", func(t *testing.T) {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumerGroup("test-group"),
			kgo.ConsumeTopics("test-topic"),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			// RebalanceTimeout not set - uses default (60s)
		)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		output := captureLogOutput(func() {
			warnIfRebalanceTimeoutTooLow(client)
		})

		if strings.Contains(output, "RebalanceTimeout") {
			t.Errorf("should not warn when using default: %s", output)
		}
	})
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
