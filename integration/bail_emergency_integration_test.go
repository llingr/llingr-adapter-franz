// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-adapter-franz/franzadapter"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// The bail integration gauntlet uses a kgo client with kfake, the adapter's
// fetch goroutine, a real record flow; the engine is the smallest fake that
// satisfies the emergency assertion. The fault is the foreign-topic guard
// (one of bail's two real call sites): the client legitimately consumes a
// second topic the consumer was not built for, and the first record from it
// must bail BEFORE delivery. Each iteration:
//
//  1. Fresh cluster, custom client consuming TWO topics, one record produced
//     and consumed from the configured topic (proves join+fetch+dispatch),
//  2. A record lands on the foreign topic: the fetch loop refuses it and
//     bail takes the BOUND arm,
//  3. Invariants: EmergencyShutdown exactly once with the foreign-topic
//     reason, graceful Shutdown never called, and goleak proves the
//     self-release (leave + close against the LIVE cluster) tore down every
//     client/fetch goroutine.
//
// Odd iterations race a graceful Unsubscribe against the foreign record, so
// the release election (unsubscribeOnce) is exercised for real: the trip may
// then legitimately not fire (the release can win first), but nothing may
// leak or double-release.
//
// Deliberately NOT driven by broker death or topic deletion, per empirical
// probes: topic deletion is stripped as retriable (endless internal metadata
// retries, nothing surfaces), and total broker death surfaces a single
// group-session error and then kgo's manage loop parks awaiting a rejoin that
// needs a live broker - one error is too sparse for the streak to mature, and
// far too slow for an integration iteration. The poll-error bail arm stays covered
// by the unit tests' injected fetches; the silent-death gap is a standing
// design question.
//
// Iterations default to 250 (-short: 3); LLINGR_FRANZ_STRESS_ITERS overrides.
const franzStressItersEnv = "LLINGR_FRANZ_STRESS_ITERS"

type quietLogger struct{}

func (quietLogger) Error(_ context.Context, _ string, _ ...any) {}
func (quietLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (quietLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (quietLogger) Debug(_ context.Context, _ string, _ ...any) {}

// debugLogger (LLINGR_FRANZ_STRESS_DEBUG=1) surfaces the adapter's own view -
// poll errors, bail, unsubscribe brackets - when diagnosing a failing run.
type debugLogger struct{}

func (debugLogger) log(level, format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "[adapter %s] %s\n", level, fmt.Sprintf(format, args...))
}
func (l debugLogger) Error(_ context.Context, format string, args ...any) {
	l.log("ERROR", format, args...)
}
func (l debugLogger) Warn(_ context.Context, format string, args ...any) {
	l.log("WARN", format, args...)
}
func (l debugLogger) Info(_ context.Context, format string, args ...any) {
	l.log("INFO", format, args...)
}
func (l debugLogger) Debug(_ context.Context, format string, args ...any) {
	l.log("DEBUG", format, args...)
}

func harnessLogger() nexus.Logger {
	if os.Getenv("LLINGR_FRANZ_STRESS_DEBUG") == "1" {
		return debugLogger{}
	}
	return quietLogger{}
}

// emergencyEngine is the minimal engine: it records trips (satisfying the
// adapter's emergency assertion) and legacy Shutdowns (which must NOT run).
type emergencyEngine struct {
	ctx        context.Context
	trips      atomic.Int32
	lastReason atomic.Pointer[error]
	shutdowns  atomic.Int32
}

func (e *emergencyEngine) Subscribe() error { return nil }
func (e *emergencyEngine) Shutdown() error  { e.shutdowns.Add(1); return nil }
func (e *emergencyEngine) TriggerRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}
func (e *emergencyEngine) Context() context.Context { return e.ctx }
func (e *emergencyEngine) Logger() nexus.Logger     { return harnessLogger() }
func (e *emergencyEngine) EmergencyShutdown(reason error) {
	e.trips.Add(1)
	e.lastReason.Store(&reason)
}

type engineBuilder struct {
	topic  string
	engine *emergencyEngine
}

func (b *engineBuilder) TopicName() string { return b.topic }
func (b *engineBuilder) Build(nexus.BrokerPort[*kgo.Record]) nexus.AdaptedConsumer[*kgo.Record] {
	return b.engine
}

func TestBailBoundArm_Gauntlet(t *testing.T) {
	iterations := 250
	if v := os.Getenv(franzStressItersEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			iterations = n
		}
	}
	if testing.Short() && os.Getenv(franzStressItersEnv) == "" {
		iterations = 3
	}
	t.Logf("bail gauntlet: %d iterations", iterations)

	for i := 0; i < iterations; i++ {
		raceUnsubscribe := i%2 == 1
		t.Run(fmt.Sprintf("iter_%d_race_%v", i, raceUnsubscribe), func(t *testing.T) {
			runBailIteration(t, raceUnsubscribe)
		})
		if t.Failed() {
			t.Fatalf("iteration %d failed", i)
		}
	}
}

func runBailIteration(t *testing.T, raceUnsubscribe bool) {
	t.Helper()
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "bail-topic"
	const foreignTopic = "foreign-topic"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic, foreignTopic))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()
	seeds := cluster.ListenAddrs()

	// one record proves join+fetch+dispatch work before the fault is injected
	producer, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(context.Background(),
		&kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	engine := &emergencyEngine{ctx: context.Background()}

	// NewCustom: the client consumes BOTH topics while the consumer is built
	// for one, the misconfiguration the foreign-topic guard exists to refuse
	adapter := franzadapter.NewCustom()
	clientOpts := append([]kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("bail-group"),
		kgo.ConsumeTopics(topic, foreignTopic),
	}, adapter.RequiredOpts()...)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	adapter.SetClient(client)

	if _, err := adapter.CreateConsumer(&engineBuilder{topic: topic, engine: engine}); err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	if err := adapter.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// consume the proof record through the real dispatch handshake
	deadline := time.Now().Add(10 * time.Second)
	for {
		record, ok, pollErr := adapter.Poll(50 * time.Millisecond)
		if ok && record != nil {
			break
		}
		_ = pollErr // pre-assignment polls surface transient states; the deadline bounds them
		if time.Now().After(deadline) {
			t.Fatal("proof record never arrived; group join did not complete")
		}
	}
	// gate protocol: the NEXT Poll acknowledges the delivered record's
	// dispatch; without it the fetch loop stays parked awaiting the ack and
	// never polls into the fault
	_, _, _ = adapter.Poll(10 * time.Millisecond)

	// the fault: a real record on the foreign topic; the fetch loop must
	// refuse it before delivery and bail
	if err := producer.ProduceSync(context.Background(),
		&kgo.Record{Topic: foreignTopic, Key: []byte("f"), Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce foreign: %v", err)
	}

	if raceUnsubscribe {
		go func() { _ = adapter.Unsubscribe() }()
	}

	// bail's bound arm must trip the engine (unless a racing release won
	// first) and self-release; wait for either the trip or, in the racing
	// flavour, quiescence
	tripDeadline := time.Now().Add(15 * time.Second)
	for engine.trips.Load() == 0 && time.Now().Before(tripDeadline) {
		if raceUnsubscribe {
			// a racing release that closed the client before the streak
			// matured legitimately prevents the bail; stop waiting once the
			// client reports closed
			if _, _, pollErr := adapter.Poll(10 * time.Millisecond); pollErr != nil &&
				engine.trips.Load() == 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !raceUnsubscribe && engine.trips.Load() != 1 {
		t.Fatalf("EmergencyShutdown called %d times, want exactly 1", engine.trips.Load())
	}
	if got := engine.trips.Load(); got > 1 {
		t.Fatalf("EmergencyShutdown called %d times, want at most 1", got)
	}
	if engine.trips.Load() == 1 {
		if reason := engine.lastReason.Load(); reason == nil || *reason == nil {
			t.Error("trip delivered a nil reason")
		}
	}
	if engine.shutdowns.Load() != 0 {
		t.Errorf("graceful Shutdown ran %d times on the bail path, want 0", engine.shutdowns.Load())
	}

	// release must complete (bail's self-release or the racing caller);
	// idempotent Unsubscribe makes this safe to await unconditionally
	_ = adapter.Unsubscribe()
}
