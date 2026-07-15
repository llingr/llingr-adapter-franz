// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"testing"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// panicOnBuildBuilder is a ConsumerBuilder whose Build panics, standing in for
// the engine panicking on an out-of-range DemuxConfig inside builder.Build.
type panicOnBuildBuilder struct{ topic string }

func (b *panicOnBuildBuilder) TopicName() string { return b.topic }

func (b *panicOnBuildBuilder) Build(nexus.BrokerPort[*kgo.Record]) nexus.AdaptedConsumer[*kgo.Record] {
	panic("build boom")
}

// On the New path, CreateConsumer opens the kgo client itself (initClient) and
// then calls builder.Build. If Build panics, the adapter must close the client
// it opened before the panic propagates, or the client's background goroutines
// leak. This drives the REAL createdHere path against kfake (franz-go's
// in-process broker, so initClient's Ping succeeds) and uses goleak to assert
// no client goroutine survives the panic — precisely the leak the fix prevents.
// It is the end-to-end counterpart to the kafka adapter's mock-builder test,
// which covers the equivalent ownsConsumer path with an observable closeFn.
func TestCreateConsumerClosesClientOnBuildPanic(t *testing.T) {
	// Baseline the runtime/prior-test goroutines; cluster.Close (deferred
	// below, so it runs first) reaps kfake's, leaving goleak to flag only a
	// client goroutine that outlived the panic.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "test-topic"))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()

	adapter := franzadapter.New(context.Background(), "test-group", cluster.ListenAddrs()...)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the build panic to propagate")
			}
		}()
		_, _ = adapter.CreateConsumer(&panicOnBuildBuilder{topic: "test-topic"})
		t.Fatal("CreateConsumer should have panicked")
	}()
}
