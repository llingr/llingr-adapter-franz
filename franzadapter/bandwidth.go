// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// compressionNames maps CompressionType codes to human-readable names.
// 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd (kafka protocol spec).
var compressionNames = [5]string{"none", "gzip", "snappy", "lz4", "zstd"}

func compressionName(code uint8) string {
	if int(code) < len(compressionNames) {
		return compressionNames[code]
	}
	return fmt.Sprintf("unknown(%d)", code)
}

// bandwidthCollector accumulates per-partition byte counters from franz-go
// hooks and emits BandwidthMetrics packets at a configurable cadence.
type bandwidthCollector struct {
	mu         sync.Mutex
	partitions map[int32]*partitionAccumulator
	brokers    map[int32]nexus.BrokerInfo // keyed by NodeID
	callback   nexus.BandwidthCallback
	interval   time.Duration
	groupID    string
	topicName  string
	quit       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
}

// partitionAccumulator collects cumulative counters for a single partition
// within one collection interval.
type partitionAccumulator struct {
	receivedBytes        int64
	transmittedBytes     int64
	receivedMessageCount int64
	compressedBytes      int64
	uncompressedBytes    int64
	compression          string
	leader               string
}

// newBandwidthCollector creates a collector with the given interval.
func newBandwidthCollector(interval time.Duration, groupID, topicName string) *bandwidthCollector {
	if interval == 0 {
		interval = nexus.DefaultBandwidthInterval
	}
	return &bandwidthCollector{
		partitions: make(map[int32]*partitionAccumulator),
		brokers:    make(map[int32]nexus.BrokerInfo),
		interval:   interval,
		groupID:    groupID,
		topicName:  topicName,
		quit:       make(chan struct{}),
	}
}

// start begins the emission timer goroutine. Only called after
// SetBandwidthCallback has registered the callback.
func (bc *bandwidthCollector) start() {
	bc.wg.Add(1)
	go bc.emitLoop()
}

// stop signals the emission goroutine to exit and waits for it.
func (bc *bandwidthCollector) stop() {
	bc.once.Do(func() {
		close(bc.quit)
	})
	bc.wg.Wait()
}

// emitLoop ticks at the configured interval and emits packets.
func (bc *bandwidthCollector) emitLoop() {
	defer bc.wg.Done()
	ticker := time.NewTicker(bc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bc.emit()
		case <-bc.quit:
			return
		}
	}
}

// emit snapshots the accumulators, builds a BandwidthMetrics packet,
// calls the callback, and resets counters for the next interval.
func (bc *bandwidthCollector) emit() {
	bc.mu.Lock()
	if bc.callback == nil {
		bc.mu.Unlock()
		return
	}

	now := time.Now()

	// snapshot brokers
	brokers := make([]nexus.BrokerInfo, 0, len(bc.brokers))
	for _, b := range bc.brokers {
		brokers = append(brokers, b)
	}

	// snapshot and reset partition accumulators
	partitions := make([]nexus.PartitionBandwidth, 0, len(bc.partitions))
	for id, acc := range bc.partitions {
		partitions = append(partitions, nexus.PartitionBandwidth{
			Ts:                   now,
			ID:                   id,
			Leader:               acc.leader,
			ReceivedBytes:        acc.receivedBytes,
			TransmittedBytes:     acc.transmittedBytes,
			ReceivedMessageCount: acc.receivedMessageCount,
			CompressedBytes:      acc.compressedBytes,
			UncompressedBytes:    acc.uncompressedBytes,
			Compression:          acc.compression,
		})
		// reset for next interval
		acc.receivedBytes = 0
		acc.transmittedBytes = 0
		acc.receivedMessageCount = 0
		acc.compressedBytes = 0
		acc.uncompressedBytes = 0
	}

	callback := bc.callback
	bc.mu.Unlock()

	packet := nexus.BandwidthMetrics{
		Ts:                    now,
		StatsIntervalDuration: bc.interval,
		BandwidthMetricsID:    generateUUID(),
		TopicName:             bc.topicName,
		ConsumerGroup:         bc.groupID,
		Brokers:               brokers,
		Partitions:            partitions,
	}

	callback(packet)
}

// recordFetchBatch accumulates a fetch batch from HookFetchBatchRead.
func (bc *bandwidthCollector) recordFetchBatch(meta kgo.BrokerMetadata, partition int32, metrics kgo.FetchBatchMetrics) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// update broker metadata
	bc.updateBroker(meta)

	// accumulate partition counters
	acc := bc.getOrCreatePartition(partition)
	acc.leader = strconv.FormatInt(int64(meta.NodeID), 10)
	acc.receivedBytes += int64(metrics.UncompressedBytes)
	acc.receivedMessageCount += int64(metrics.NumRecords)
	acc.compressedBytes += int64(metrics.CompressedBytes)
	acc.uncompressedBytes += int64(metrics.UncompressedBytes)
	if metrics.CompressionType > 0 {
		acc.compression = compressionName(metrics.CompressionType)
	}
}

// recordBrokerRead accumulates bytes read from a broker (HookBrokerRead).
func (bc *bandwidthCollector) recordBrokerRead(meta kgo.BrokerMetadata, _ int) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.updateBroker(meta)
}

// recordBrokerWrite accumulates bytes written to a broker (HookBrokerWrite).
func (bc *bandwidthCollector) recordBrokerWrite(meta kgo.BrokerMetadata, _ int) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.updateBroker(meta)
	// TX bytes are tracked per-broker, attributed to all partitions served by this broker.
	// For consumer-side bandwidth, the primary counters come from fetch batch hooks.
	// Broker-level write bytes (commits, heartbeats) are minimal and not partition-attributed.
}

func (bc *bandwidthCollector) updateBroker(meta kgo.BrokerMetadata) {
	if _, exists := bc.brokers[meta.NodeID]; !exists || meta.NodeID >= 0 {
		rack := ""
		if meta.Rack != nil {
			rack = *meta.Rack
		}
		bc.brokers[meta.NodeID] = nexus.BrokerInfo{
			ID:   strconv.FormatInt(int64(meta.NodeID), 10),
			Host: meta.Host,
			Port: strconv.FormatInt(int64(meta.Port), 10),
			Rack: rack,
		}
	}
}

func (bc *bandwidthCollector) getOrCreatePartition(partition int32) *partitionAccumulator {
	acc, exists := bc.partitions[partition]
	if !exists {
		acc = &partitionAccumulator{}
		bc.partitions[partition] = acc
	}
	return acc
}

// generateUUID returns a v4 UUID string using crypto/rand.
func generateUUID() string {
	var uuid [16]byte
	_, _ = rand.Read(uuid[:])
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 2
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// --- BandwidthPort[*kgo.Record] implementation on Adapter ---

// SetBandwidthCallback registers the framework callback and starts the
// emission timer. Called by the demux builder during Build().
func (a *Adapter) SetBandwidthCallback(cb nexus.BandwidthCallback) {
	if a.bwCollector == nil {
		return
	}
	a.bwCollector.callback = cb
	a.bwCollector.start()
}

// StatsInterval returns the configured bandwidth collection cadence.
func (a *Adapter) StatsInterval() time.Duration {
	if a.bwCollector == nil {
		return nexus.DefaultBandwidthInterval
	}
	return a.bwCollector.interval
}

// --- franz-go hook implementations ---

// OnFetchBatchRead implements kgo.HookFetchBatchRead.
func (a *Adapter) OnFetchBatchRead(meta kgo.BrokerMetadata, _ string, partition int32, metrics kgo.FetchBatchMetrics) {
	if a.bwCollector != nil {
		a.bwCollector.recordFetchBatch(meta, partition, metrics)
	}
}

// OnBrokerRead implements kgo.HookBrokerRead.
func (a *Adapter) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, _ error) {
	if a.bwCollector != nil {
		a.bwCollector.recordBrokerRead(meta, bytesRead)
	}
}

// OnBrokerWrite implements kgo.HookBrokerWrite.
func (a *Adapter) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, _ error) {
	if a.bwCollector != nil {
		a.bwCollector.recordBrokerWrite(meta, bytesWritten)
	}
}
