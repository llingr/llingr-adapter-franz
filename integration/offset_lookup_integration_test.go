// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// assignRecorder is the minimal engine that captures the RebalanceInfo the
// adapter delivers on assignment; assigned closes once an Assign arrives.
type assignRecorder struct {
	ctx      context.Context
	mu       sync.Mutex
	assigns  []nexus.RebalanceInfo
	assigned chan struct{}
	once     sync.Once
}

func (r *assignRecorder) Subscribe() error { return nil }
func (r *assignRecorder) Shutdown() error  { return nil }
func (r *assignRecorder) TriggerRebalance(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	if rt == nexus.Assign {
		r.mu.Lock()
		r.assigns = append(r.assigns, info...)
		r.mu.Unlock()
		r.once.Do(func() { close(r.assigned) })
	}
	return nil
}
func (r *assignRecorder) Context() context.Context { return r.ctx }
func (r *assignRecorder) Logger() nexus.Logger     { return harnessLogger() }

// EmergencyShutdown satisfies the contract CreateConsumer enforces; no bail
// is expected in this test.
func (r *assignRecorder) EmergencyShutdown(error) {}

type assignRecorderBuilder struct {
	topic  string
	engine *assignRecorder
}

func (b *assignRecorderBuilder) TopicName() string { return b.topic }
func (b *assignRecorderBuilder) Build(nexus.BrokerPort[*kgo.Record]) nexus.AdaptedConsumer[*kgo.Record] {
	return b.engine
}

// The assignment offset lookup end to end against a real coordinator: offsets
// committed for the group BEFORE it joins must arrive in the Assign event's
// RebalanceInfo.CommittedOffset, and a partition with no committed offset must
// carry the -1 unknown sentinel. This exercises newOffsetFetcher's production
// path in full: group resolution (the NewCustom client-group fallback), the
// coordinator OffsetFetch, and the response projection.
func TestAssignmentOffsetLookup_SeedsCommittedBaselines(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		topic           = "lookup-topic"
		group           = "lookup-group"
		committedOffset = int64(7)
	)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()
	seeds := cluster.ListenAddrs()

	// commit an offset for partition 0 only, before the group has any member
	admin, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admin.Close()
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: committedOffset})
	responses, err := kadm.NewClient(admin).CommitOffsets(context.Background(), group, offsets)
	if err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if err := responses.Error(); err != nil {
		t.Fatalf("CommitOffsets response: %v", err)
	}

	engine := &assignRecorder{ctx: context.Background(), assigned: make(chan struct{})}
	adapter := franzadapter.NewCustom()
	clientOpts := append([]kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	}, adapter.RequiredOpts()...)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	adapter.SetClient(client)

	if _, err := adapter.CreateConsumer(&assignRecorderBuilder{topic: topic, engine: engine}); err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	if err := adapter.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = adapter.Unsubscribe() }()

	// assigns are delivered through the poll path; drive it until one lands
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, _, _ = adapter.Poll(50 * time.Millisecond)
		select {
		case <-engine.assigned:
		default:
			if time.Now().After(deadline) {
				t.Fatal("assignment never arrived")
			}
			continue
		}
		break
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	byPartition := make(map[int32]int64, len(engine.assigns))
	for _, info := range engine.assigns {
		if info.TopicName != topic {
			t.Fatalf("assign for unexpected topic %q", info.TopicName)
		}
		byPartition[info.Partition] = info.CommittedOffset
	}
	if len(byPartition) != 2 {
		t.Fatalf("want both partitions assigned, got %v", byPartition)
	}
	if got := byPartition[0]; got != committedOffset {
		t.Fatalf("partition 0 committed baseline: got %d, want %d", got, committedOffset)
	}
	if got := byPartition[1]; got != -1 {
		t.Fatalf("partition 1 (never committed) baseline: got %d, want -1", got)
	}
}
