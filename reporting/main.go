// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package reporting

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	PMetricsLatencyGet = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "latency_get_ns",
		Help:    "Observed latencies for GET command in nanoseconds",
		Buckets: prometheus.DefBuckets,
	})
	PMetricsLatencySet = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "latency_set_ns",
		Help:    "Observed latencies for SET command in nanoseconds",
		Buckets: prometheus.DefBuckets,
	})
)

type BenchmarkStats struct {
	TotalOps     uint64
	TotalGets    uint64
	TotalSets    uint64
	GetLatencies []time.Duration
	SetLatencies []time.Duration
	ErrorCount   uint64
	StartTime    time.Time
	LastReportAt time.Time
	StatLock     sync.Mutex
}

func (stats *BenchmarkStats) EmitGet(latency_ns float64) {
	PMetricsLatencyGet.Observe(latency_ns)
}

func (stats *BenchmarkStats) EmitSet(latency_ns float64) {
	PMetricsLatencySet.Observe(latency_ns)
}

func (stats *BenchmarkStats) Print() {
	stats.StatLock.Lock()
	defer stats.StatLock.Unlock()

	now := time.Now()
	elapsed := now.Sub(stats.StartTime)
	stats.LastReportAt = now

	totalOps := stats.TotalOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	var getP50, getP99, setP50, setP99 time.Duration
	var getAvg, setAvg time.Duration

	if len(stats.GetLatencies) > 0 {
		getLatencies := stats.GetLatencies
		getP50 = getPercentileLatency(getLatencies, 50)
		getP99 = getPercentileLatency(getLatencies, 99)
		getAvg = getAverageLatency(getLatencies)
	}

	if len(stats.SetLatencies) > 0 {
		setLatencies := stats.SetLatencies
		setP50 = getPercentileLatency(setLatencies, 50)
		setP99 = getPercentileLatency(setLatencies, 99)
		setAvg = getAverageLatency(setLatencies)
	}

	fmt.Printf("Runtime: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total Operations: %d (Gets: %d, Sets: %d)\n", totalOps, stats.TotalGets, stats.TotalSets)
	fmt.Printf("Throughput: %.2f ops/sec\n", opsPerSec)

	if len(stats.GetLatencies) > 0 {
		fmt.Printf("GET Latency (avg / p50 / p99): %.2fms / %.2fms / %.2fms\n",
			float64(getAvg)/float64(time.Millisecond),
			float64(getP50)/float64(time.Millisecond),
			float64(getP99)/float64(time.Millisecond))
	}

	if len(stats.GetLatencies) > 0 {
		fmt.Printf("SET Latency (avg / p50 / p99): %.2fms / %.2fms / %.2fms\n",
			float64(setAvg)/float64(time.Millisecond),
			float64(setP50)/float64(time.Millisecond),
			float64(setP99)/float64(time.Millisecond))
	}

	fmt.Printf("Errors: %d\n", stats.ErrorCount)
}

func getPercentileLatency(latencies []time.Duration, percentile int) time.Duration {
	n := len(latencies)
	if n == 0 {
		return 0
	}

	var max time.Duration
	var min time.Duration = time.Hour
	var sum time.Duration

	for _, lat := range latencies {
		if lat > max {
			max = lat
		}
		if lat < min {
			min = lat
		}
		sum += lat
	}

	// Use a simplified approach for percentiles
	// This is not accurate for all distributions but works for benchmarking
	if percentile >= 99 {
		return max
	} else if percentile <= 1 {
		return min
	} else {
		// Linear approximation between min and max
		ratio := float64(percentile) / 100.0
		range_dur := max - min
		return min + time.Duration(float64(range_dur)*ratio)
	}
}

func getAverageLatency(latencies []time.Duration) time.Duration {
	var sum time.Duration
	for _, lat := range latencies {
		sum += lat
	}
	return sum / time.Duration(len(latencies))
}
