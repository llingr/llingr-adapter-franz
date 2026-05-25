// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

// Package franzadapter implements nexus.BrokerPort for franz-go.
//
// franz-go is a pure Go Kafka client (no CGO) that works with
// Kafka, RedPanda, Amazon MSK, and any Kafka-compatible broker.
//
// # Quick Start
//
// For most use cases, use New() which handles all client configuration.
// Topic name is provided via the builder's WithTopicName(), not here.
//
//	builder := demux.NewBuilder(processOrder, handleDeadLetter).
//	    WithTopicName("orders")
//
//	adapter := franzadapter.New(ctx, "my-group",
//	    "broker-1:9092", "broker-2:9092", "broker-3:9092")
//
//	consumer, err := adapter.CreateConsumer(builder)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	consumer.Subscribe()
//
// # Authentication
//
// Use NewWithOptions() for SASL, TLS, or other custom settings:
//
//	adapter := franzadapter.NewWithOptions(ctx, "my-group",
//	    []string{"broker:9093"},
//	    []kgo.Opt{
//	        kgo.SASL(scram.Auth{User: "u", Pass: "p"}.AsSha256Mechanism()),
//	        kgo.DialTLSConfig(tlsConfig),
//	    },
//	)
//
// # Full Client Control
//
// For complete control over kgo.Client configuration, use NewCustom():
//
//	adapter := franzadapter.NewCustom()
//
//	// Topic must match builder.WithTopicName()
//	client, err := kgo.NewClient(
//	    kgo.SeedBrokers("localhost:9092"),
//	    kgo.ConsumerGroup("my-group"),
//	    kgo.ConsumeTopics("orders"),
//	    // Required options:
//	    kgo.DisableAutoCommit(),
//	    kgo.BlockRebalanceOnPoll(),
//	    kgo.OnPartitionsAssigned(adapter.OnAssigned),
//	    kgo.OnPartitionsRevoked(adapter.OnRevoked),
//	    kgo.OnPartitionsLost(adapter.OnLost),
//	    // Your custom options:
//	    kgo.MaxConcurrentFetches(2),
//	)
//
//	adapter.SetClient(client)
//	consumer, err := adapter.CreateConsumer(builder)
package franzadapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter/validate"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// baseKgoOptsCount is the number of kgo.Opt entries added by initClient:
// 4 base (brokers, group, topic, balancer) + 5 required (auto-commit,
// block-rebalance, 3 rebalance callbacks).
const baseKgoOptsCount = 9

// Adapter wraps a franz-go client implementing nexus.BrokerPort[*kgo.Record].
//
// Create with New() for standard configuration, or NewCustom() for advanced
// use cases requiring custom kgo.Client options.
type Adapter struct {
	client    *kgo.Client
	logger    nexus.Logger
	ctx       context.Context
	topicName string

	// adaptedConsumer for rebalance callbacks (has TriggerRebalance)
	adaptedConsumer nexus.AdaptedConsumer[*kgo.Record]

	// rebalance coordination
	mu              sync.Mutex
	pendingAssigned map[string][]int32
	pendingRevoked  map[string][]int32

	// deferred client creation config (for New/NewWithOptions)
	// client is created in CreateConsumer() when topic is known from builder
	initCtx  context.Context
	groupID  string
	brokers  []string
	userOpts []kgo.Opt

	// bandwidth telemetry (nil when not configured)
	bwCollector *bandwidthCollector

	// function fields for testability (wired from client)
	pollRecordsFn    func(ctx context.Context, maxPollRecords int) kgo.Fetches
	allowRebalanceFn func()
	commitRecordsFn  func(ctx context.Context, rs ...*kgo.Record) error
	closeFn          func()
}

// New creates a franz-go adapter configured for the given consumer group.
//
// This is the recommended constructor for most use cases. The kgo.Client
// is created lazily in CreateConsumer() when the topic is known from the builder.
//
// The adapter will be configured with:
//   - Consumer group membership
//   - Disabled auto-commit (required by nexus.ConsumerBuilder implementations)
//   - Cooperative sticky balancing (minimal disruption during rebalances)
//   - Rebalance callbacks properly wired to the adapter
//
// The context is used for the initial connectivity check (Ping) during
// CreateConsumer() and can be cancelled to abort connection.
//
// Topic name is provided via builder.WithTopicName(), not here.
//
// For SASL, TLS, or other custom kgo.Opt settings, use NewWithOptions().
func New(ctx context.Context, groupID string, brokers ...string) *Adapter {
	return NewWithOptions(ctx, groupID, brokers)
}

// NewWithOptions creates a franz-go adapter with custom kgo.Opt configuration.
//
// Use this when you need SASL authentication, TLS, or other custom settings.
// The kgo.Client is created lazily in CreateConsumer() when the topic is known.
//
// Options are applied after base configuration but before required options:
//   - You CAN override: Balancers, fetch settings, timeouts, SASL, TLS
//   - You CANNOT override: DisableAutoCommit, BlockRebalanceOnPoll, rebalance callbacks
//
// Topic name is provided via builder.WithTopicName(), not here.
//
// Example with SASL/SCRAM authentication:
//
//	adapter := franzadapter.NewWithOptions(ctx, "my-group",
//	    []string{"broker:9093"},
//	    kgo.SASL(scram.Auth{User: "user", Pass: "pass"}.AsSha256Mechanism()),
//	    kgo.DialTLSConfig(tlsConfig),
//	)
func NewWithOptions(ctx context.Context, groupID string, brokers []string, opts ...kgo.Opt) *Adapter {
	return &Adapter{
		initCtx:         ctx,
		groupID:         groupID,
		brokers:         brokers,
		userOpts:        opts,
		pendingAssigned: make(map[string][]int32),
		pendingRevoked:  make(map[string][]int32),
	}
}

// initClient creates the kgo.Client with the topic from builder.
// Called from CreateConsumer() when topic is known.
func (a *Adapter) initClient() error {
	clientOpts := make([]kgo.Opt, 0, baseKgoOptsCount+len(a.userOpts))
	clientOpts = append(clientOpts,
		kgo.SeedBrokers(a.brokers...),
		kgo.ConsumerGroup(a.groupID),
		kgo.ConsumeTopics(a.topicName),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	)

	clientOpts = append(clientOpts, a.userOpts...)

	requiredOpts := []kgo.Opt{
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(a.OnAssigned),
		kgo.OnPartitionsRevoked(a.OnRevoked),
		kgo.OnPartitionsLost(a.OnLost),
	}
	if a.bwCollector != nil {
		requiredOpts = append(requiredOpts, kgo.WithHooks(a))
	}
	clientOpts = append(clientOpts, requiredOpts...)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create kgo client: %w", err)
	}

	if err = client.Ping(a.initCtx); err != nil {
		client.Close()
		return fmt.Errorf("failed to connect to broker: %w", err)
	}

	a.client = client
	a.pollRecordsFn = client.PollRecords
	a.allowRebalanceFn = client.AllowRebalance
	a.commitRecordsFn = client.CommitRecords
	a.closeFn = client.Close
	return nil
}

// WithBandwidthInterval enables bandwidth telemetry collection at the given cadence.
// The adapter will accumulate per-partition byte counters from franz-go hooks and
// emit BandwidthMetrics packets via the callback registered by the framework.
//
// Valid range: 1s to 12h. Zero uses the default (1 minute).
// Must be called before CreateConsumer().
func (a *Adapter) WithBandwidthInterval(d time.Duration) *Adapter {
	if err := nexus.ValidateBandwidthInterval(d); err != nil {
		panic(fmt.Errorf("WithBandwidthInterval: %w", err))
	}
	a.bwCollector = newBandwidthCollector(d, a.groupID, "")
	return a
}

// NewCustom creates a franz-go adapter for use with a custom kgo.Client.
//
// This is for advanced use cases where you need kgo.Client configuration
// beyond what New() provides (e.g., custom fetch settings, SASL auth,
// TLS configuration).
//
// # Two-Phase Setup Required
//
// Because franz-go requires rebalance callbacks at client creation time,
// you must follow this sequence:
//
//  1. Call NewCustom() to create the adapter (without client)
//  2. Create your kgo.Client with adapter.OnAssigned/OnRevoked/OnLost
//  3. Call adapter.SetClient(client) to complete setup
//
// # Required kgo.Client Options
//
// Your client MUST include these options for correct operation:
//
//	kgo.DisableAutoCommit()                      // required for nexus consumers
//	kgo.BlockRebalanceOnPoll()                   // cooperative rebalancing
//	kgo.OnPartitionsAssigned(adapter.OnAssigned) // rebalance handling
//	kgo.OnPartitionsRevoked(adapter.OnRevoked)   // rebalance handling
//	kgo.OnPartitionsLost(adapter.OnLost)         // rebalance handling
//
// # Recommended Options
//
//	kgo.Balancers(kgo.CooperativeStickyBalancer()) // minimal disruption
//
// See package documentation for a complete example.
func NewCustom() *Adapter {
	return &Adapter{
		pendingAssigned: make(map[string][]int32),
		pendingRevoked:  make(map[string][]int32),
	}
}

// SetClient attaches a kgo.Client to an adapter created with NewCustom.
//
// The client must be configured with the adapter's rebalance callbacks
// (OnAssigned, OnRevoked, OnLost). See NewCustom documentation for
// required configuration.
//
// This method should only be called once, immediately after creating
// the kgo.Client.
func (a *Adapter) SetClient(client *kgo.Client) {
	a.client = client
	a.pollRecordsFn = client.PollRecords
	a.allowRebalanceFn = client.AllowRebalance
	a.commitRecordsFn = client.CommitRecords
	a.closeFn = client.Close
}

// CreateConsumer wires the builder to this adapter via Port-Binding Builder pattern.
//
// The builder carries application dependencies (processMessage, deadLetter, etc.).
// This method injects the adapter as BrokerPort. Topic name is obtained from
// builder.TopicName().
//
// For adapters created with New()/NewWithOptions(), this is where the kgo.Client
// is actually created and connected (using the topic from the builder).
//
// Returns an error if client creation or connection fails.
// Returns Consumer (not AdaptedConsumer) to hide adapter-internal methods
// like TriggerRebalance from the host application.
func (a *Adapter) CreateConsumer(builder nexus.ConsumerBuilder[*kgo.Record]) (nexus.Consumer[*kgo.Record], error) {
	a.topicName = builder.TopicName()
	if a.bwCollector != nil {
		a.bwCollector.topicName = a.topicName
	}

	// If client not yet created (New/NewWithOptions path), create it now
	if a.client == nil && a.groupID != "" {
		if err := a.initClient(); err != nil {
			return nil, err
		}
	}

	// Validate we have a client (either from NewCustom+SetClient or New/NewWithOptions)
	if a.client == nil {
		return nil, fmt.Errorf("no kgo.Client configured: use New(), NewWithOptions(), or NewCustom()+SetClient()")
	}

	// Validate client configuration - panics on critical misconfigurations, warns on suboptimal
	validate.Client(a.client)

	a.adaptedConsumer = builder.Build(a)
	a.ctx = a.adaptedConsumer.Context()
	a.logger = a.adaptedConsumer.Logger()
	return a.adaptedConsumer, nil
}
