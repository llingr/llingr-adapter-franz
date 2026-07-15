// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// mapCommittedOffsets is the pure projection behind the offset fetcher: keep
// per-partition offsets, drop partitions whose response carries an error
// (absence means unknown, -1, to callers).
func TestMapCommittedOffsets(t *testing.T) {
	response := func(at int64, err error) kadm.OffsetResponse {
		return kadm.OffsetResponse{Offset: kadm.Offset{At: at}, Err: err}
	}

	cases := []struct {
		name string
		in   kadm.OffsetResponses
		want map[string]map[int32]int64
	}{
		{
			name: "empty response",
			in:   kadm.OffsetResponses{},
			want: map[string]map[int32]int64{},
		},
		{
			name: "offsets kept per topic and partition",
			in: kadm.OffsetResponses{
				"orders":   {0: response(41, nil), 1: response(0, nil)},
				"payments": {3: response(7, nil)},
			},
			want: map[string]map[int32]int64{
				"orders":   {0: 41, 1: 0},
				"payments": {3: 7},
			},
		},
		{
			name: "errored partition omitted, healthy sibling kept",
			in: kadm.OffsetResponses{
				"orders": {
					0: response(41, nil),
					1: response(99, context.DeadlineExceeded),
				},
			},
			want: map[string]map[int32]int64{"orders": {0: 41}},
		},
		{
			name: "topic with only errored partitions keeps an empty entry",
			in: kadm.OffsetResponses{
				"orders": {0: response(5, context.DeadlineExceeded)},
			},
			want: map[string]map[int32]int64{"orders": {}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapCommittedOffsets(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("topics: got %v, want %v", got, tc.want)
			}
			for topic, wantPartitions := range tc.want {
				gotPartitions, ok := got[topic]
				if !ok {
					t.Fatalf("topic %q missing: got %v", topic, got)
				}
				if len(gotPartitions) != len(wantPartitions) {
					t.Fatalf("topic %q partitions: got %v, want %v", topic, gotPartitions, wantPartitions)
				}
				for partition, want := range wantPartitions {
					if got := gotPartitions[partition]; got != want {
						t.Fatalf("topic %q partition %d: got %d, want %d", topic, partition, got, want)
					}
				}
			}
		})
	}
}

// unconnectedClient builds a real kgo.Client that never dials: construction is
// lazy in franz-go, so group-resolution paths run without a broker.
func unconnectedClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("localhost:1")}, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// Without a group on the adapter or the client, the fetcher must fail fast
// with a configuration error, before any broker round trip.
func TestOffsetFetcher_NoGroupConfigured(t *testing.T) {
	adapter := NewCustom()
	fetch := adapter.newOffsetFetcher(unconnectedClient(t))

	_, err := fetch(context.Background(), []string{"orders"})
	if err == nil || !strings.Contains(err.Error(), "no consumer group configured") {
		t.Fatalf("want a no-consumer-group error, got %v", err)
	}
}

// With the adapter's own group set, resolution succeeds and the broker fetch
// runs; a cancelled context surfaces its error to the caller.
func TestOffsetFetcher_AdapterGroupFetchErrorSurfaces(t *testing.T) {
	adapter := NewCustom()
	adapter.groupID = "unit-group"
	fetch := adapter.newOffsetFetcher(unconnectedClient(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fetch(ctx, []string{"orders"})
	if err == nil {
		t.Fatal("want an error from the cancelled fetch, got nil")
	}
	if strings.Contains(err.Error(), "no consumer group configured") {
		t.Fatalf("group resolution failed despite adapter group: %v", err)
	}
}

// A NewCustom adapter with no group of its own must fall back to the group
// configured on the kgo.Client (the documented NewCustom arrangement).
func TestOffsetFetcher_ClientGroupFallback(t *testing.T) {
	adapter := NewCustom()
	client := unconnectedClient(t,
		kgo.ConsumerGroup("client-group"),
		kgo.ConsumeTopics("orders"),
	)
	fetch := adapter.newOffsetFetcher(client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fetch(ctx, []string{"orders"})
	if err == nil {
		t.Fatal("want an error from the cancelled fetch, got nil")
	}
	if strings.Contains(err.Error(), "no consumer group configured") {
		t.Fatalf("client-group fallback did not resolve: %v", err)
	}
}
