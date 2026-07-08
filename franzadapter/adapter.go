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
//	// builder is your nexus.ConsumerBuilder
//	builder := NewBuilder(processOrder, handleDeadLetter).
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
//	// Topic must match builder.WithTopicName(). adapter.RequiredOpts() carries
//	// the auto-commit, block-rebalance, and rebalance-callback options.
//	opts := append([]kgo.Opt{
//	    kgo.SeedBrokers("localhost:9092"),
//	    kgo.ConsumerGroup("my-group"),
//	    kgo.ConsumeTopics("orders"),
//	    kgo.MaxConcurrentFetches(2), // your custom options
//	}, adapter.RequiredOpts()...)
//	client, err := kgo.NewClient(opts...)
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
	"github.com/twmb/franz-go/pkg/kadm"
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

	// rebalance coordination. Only assigns are queued for the poll loop;
	// pendingRevoked stays deliberately empty: revokes are drained and
	// committed synchronously inside OnRevoked (franz-go blocks the group's
	// reassignment only until that callback returns, so a queued revoke
	// would race the handoff and duplicate the uncommitted tail). The field
	// remains to document that asymmetry, and tests pin its emptiness.
	mu              sync.Mutex
	pendingAssigned map[string][]int32
	pendingRevoked  map[string][]int32

	// pendingRecord holds a fetched-but-undelivered record across a failed
	// assign trigger: the client already consumed it, so dropping it on the
	// error path would violate at-least-once. Poll-goroutine only, no lock.
	pendingRecord *kgo.Record

	// DIAGNOSTIC TRACE field commented out for the mutex-vs-coincidence experiment (see the
	// note at the traceFirstReadAfterAssign call in broker_port.go Poll). Re-enable with the
	// trace fn/call in broker_port.go and the arming in rebalance.go.
	// traceAwaitFirstRead map[int32]bool

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

	// fetchCommittedFn queries the group's committed offsets for the given topics
	// (the assignment-time baseline lookup, one coordinator round trip per assign
	// event). Wired to the admin-client fetch in initClient/SetClient; a function
	// field for testability. Skipped when WithAssignmentOffsetLookup(false).
	fetchCommittedFn func(ctx context.Context, topics []string) (map[string]map[int32]int64, error)

	// poll-error handling: rate-limit repeated per-partition fetch-error logs
	// and stop the consumer if a partition keeps failing. Keyed "topic-partition".
	pollErrSince  map[string]time.Time // start of the current error streak
	pollErrLogged map[string]time.Time // last time we logged that partition's error
	pollErrLastID map[string]string    // identity of that last logged error (changed error logs immediately)
	opts          adapterOptions       // poll-error tuning (defaults + validation in adapter_options.go)
	bailOnce      sync.Once
}

// New creates a franz-go adapter for the given consumer group. The kgo.Client is
// built lazily in CreateConsumer() (topic from the builder) with auto-commit
// disabled, cooperative-sticky balancing, and the rebalance callbacks wired.
//
// ctx is used for the CreateConsumer() Ping and can be cancelled to abort it.
// Topic comes from builder.WithTopicName(). For SASL/TLS, use NewWithOptions().
func New(ctx context.Context, groupID string, brokers ...string) *Adapter {
	return NewWithOptions(ctx, groupID, brokers)
}

// NewWithOptions creates an adapter with extra kgo.Opts (SASL, TLS, timeouts,
// ...). The client is built lazily in CreateConsumer().
//
// Your options apply after the base config but before the required ones:
//   - You CAN override: Balancers, fetch settings, timeouts, SASL, TLS
//   - You CANNOT override: DisableAutoCommit, BlockRebalanceOnPoll, rebalance callbacks
//
// Topic comes from builder.WithTopicName(). Example with SASL/SCRAM:
//
//	adapter := franzadapter.NewWithOptions(ctx, "my-group",
//	    []string{"broker:9093"},
//	    kgo.SASL(scram.Auth{User: "user", Pass: "pass"}.AsSha256Mechanism()),
//	    kgo.DialTLSConfig(tlsConfig),
//	)
func NewWithOptions(ctx context.Context, groupID string, brokers []string, opts ...kgo.Opt) *Adapter {
	if groupID == "" {
		panic("franzadapter: New/NewWithOptions require a consumer group")
	}
	return &Adapter{
		initCtx:         ctx,
		groupID:         groupID,
		brokers:         brokers,
		userOpts:        opts,
		pendingAssigned: make(map[string][]int32),
		pendingRevoked:  make(map[string][]int32),
		pollErrSince:    make(map[string]time.Time),
		pollErrLogged:   make(map[string]time.Time),
		pollErrLastID:   make(map[string]string),
		opts:            defaultAdapterOptions(),
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

	requiredOpts := a.RequiredOpts()
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
	a.fetchCommittedFn = a.newOffsetFetcher(client)
	return nil
}

// newOffsetFetcher returns the production fetchCommittedFn: an OffsetFetch for
// the consumer group, routed to the group coordinator via franz-go's admin
// client package ("kadm" = Kafka admin). This is the same request type kgo
// itself issues to resolve fetch positions after an assignment, so it adds no
// new load class to the broker. Partitions with a per-partition error or no
// committed offset are omitted (callers treat absence as unknown).
func (a *Adapter) newOffsetFetcher(client *kgo.Client) func(context.Context, []string) (map[string]map[int32]int64, error) {
	adm := kadm.NewClient(client)
	return func(ctx context.Context, topics []string) (map[string]map[int32]int64, error) {
		group := a.groupID
		if group == "" {
			// NewCustom adapters configure the group on the kgo.Client directly
			if g, ok := client.OptValue(kgo.ConsumerGroup).(string); ok {
				group = g
			}
		}
		if group == "" {
			return nil, fmt.Errorf("no consumer group configured")
		}

		responses, err := adm.FetchOffsetsForTopics(ctx, group, topics...)
		if err != nil {
			return nil, err
		}

		committed := make(map[string]map[int32]int64, len(responses))
		for topic, partitions := range responses {
			partitionOffsets := make(map[int32]int64, len(partitions))
			for partition, response := range partitions {
				if response.Err != nil {
					continue // absence means unknown (-1)
				}
				partitionOffsets[partition] = response.At
			}
			committed[topic] = partitionOffsets
		}
		return committed, nil
	}
}

// WithBandwidthInterval enables bandwidth telemetry at the given cadence.
// Valid range 1s to 12h; zero uses the default (1 minute). Must be called
// before CreateConsumer().
func (a *Adapter) WithBandwidthInterval(d time.Duration) *Adapter {
	if err := nexus.ValidateBandwidthInterval(d); err != nil {
		panic(fmt.Errorf("WithBandwidthInterval: %w", err))
	}
	a.bwCollector = newBandwidthCollector(d, a.groupID, "")
	return a
}

// NewCustom creates an adapter driven by a kgo.Client you build yourself, for
// configuration beyond what New() exposes.
//
// # Two-Phase Setup Required
//
// franz-go needs the rebalance callbacks at client creation time, so:
//
//  1. Call NewCustom() to create the adapter (without client)
//  2. Build your kgo.Client, spreading adapter.RequiredOpts() into it
//  3. Call adapter.SetClient(client) to complete setup
//
// The client must include adapter.RequiredOpts().
func NewCustom() *Adapter {
	return &Adapter{
		pendingAssigned: make(map[string][]int32),
		pendingRevoked:  make(map[string][]int32),
		pollErrSince:    make(map[string]time.Time),
		pollErrLogged:   make(map[string]time.Time),
		pollErrLastID:   make(map[string]string),
		opts:            defaultAdapterOptions(),
	}
}

// WithOptions configures poll-error handling from the given options, folding
// them over the defaults and validating. Must be called before CreateConsumer.
func (a *Adapter) WithOptions(options ...AdapterOption) *Adapter {
	a.opts = processAdapterOptions(options...)
	return a
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
	a.fetchCommittedFn = a.newOffsetFetcher(client)
}

// RequiredOpts are applied automatically in New and NewWithOptions.
// Custom clients create using NewCustom must also include these.
func (a *Adapter) RequiredOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(a.OnAssigned),
		kgo.OnPartitionsRevoked(a.OnRevoked),
		kgo.OnPartitionsLost(a.OnLost),
	}
}

// CreateConsumer wires the builder to this adapter and returns the consumer.
// For New/NewWithOptions adapters it also creates and connects the kgo.Client
// (topic from the builder), returning an error if that fails.
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
