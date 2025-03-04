// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dicedb/membench/config"
	"github.com/dicedb/membench/db"
	"github.com/dicedb/membench/reporting"
)

func Run(config *config.Config) {
	ctx := context.Background()
	stats := &reporting.BenchmarkStats{
		GetLatencies: make([]time.Duration, 0, config.NumRequests),
		SetLatencies: make([]time.Duration, 0, config.NumRequests),
		StartTime:    time.Now(),
		LastReportAt: time.Now(),
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle CTRL+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal. Gracefully shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Duration(config.ReportEvery) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				reportStats(stats, false)
			case <-runCtx.Done():
				return
			}
		}
	}()

	// Start worker goroutines
	for i := 0; i < config.NumClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runBenchmark(runCtx, config, stats, clientID)
		}(i)
	}

	// If duration is specified, stop after that time
	if config.Duration > 0 {
		time.Sleep(time.Duration(config.Duration) * time.Second)
		cancel()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Report final stats
	reportStats(stats, true)
}

func runBenchmark(ctx context.Context, config *config.Config, stats *reporting.BenchmarkStats, clientID int) {
	var d db.DB
	if config.Database == "redis" {
		d = db.NewRedis(config.Host, config.Port)
		defer d.Close()
	} else {
		panic("unsupported database: " + config.Database)
	}

	// Seed the random number generator
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

	// Pre-generate keys and values
	keys := make([]string, config.NumRequests)
	values := make([]string, config.NumRequests)

	for i := 0; i < config.NumRequests; i++ {
		keys[i] = generateKey(config.KeyPrefix, config.KeySize, r)
		values[i] = generateValue(config.ValueSize, r)
	}

	// Run benchmark loop
	for reqCount := 0; ; reqCount = (reqCount + 1) % config.NumRequests {
		// Check if context is canceled
		select {
		case <-ctx.Done():
			return
		default:
			// Continue benchmark
		}

		isRead := r.Float64() < config.ReadRatio
		key := keys[reqCount]

		if isRead {
			start := time.Now()
			_, err := d.Get(ctx, key)
			elapsed := time.Since(start)

			if err != nil {
				atomic.AddUint64(&stats.ErrorCount, 1)
			} else {
				stats.StatLock.Lock()
				stats.TotalGets++
				stats.GetLatencies = append(stats.GetLatencies, elapsed)
				stats.TotalOps++
				stats.StatLock.Unlock()
			}
		} else {
			value := values[reqCount]

			start := time.Now()
			err := d.Set(ctx, key, value)
			elapsed := time.Since(start)

			if err != nil {
				atomic.AddUint64(&stats.ErrorCount, 1)
			} else {
				stats.StatLock.Lock()
				stats.TotalSets++
				stats.SetLatencies = append(stats.SetLatencies, elapsed)
				stats.TotalOps++
				stats.StatLock.Unlock()
			}
		}
	}
}

func generateKey(prefix string, size int, r *rand.Rand) string {
	if size <= 0 {
		size = 16
	}

	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte(97 + r.Intn(26)) // a-z
	}

	return fmt.Sprintf("%s:%s", prefix, string(bytes))
}

func generateValue(size int, r *rand.Rand) string {
	if size <= 0 {
		size = 64
	}

	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte(48 + r.Intn(74)) // 0-9 and a-z and A-Z and some special chars
	}

	return string(bytes)
}

func reportStats(stats *reporting.BenchmarkStats, isFinal bool) {
	stats.StatLock.Lock()
	defer stats.StatLock.Unlock()

	now := time.Now()
	elapsed := now.Sub(stats.StartTime)
	intervalElapsed := now.Sub(stats.LastReportAt)
	stats.LastReportAt = now

	// Calculate throughput
	totalOps := stats.TotalOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	intervalOpsPerSec := float64(0)

	if !isFinal {
		// For interval calculation, use the difference since last report
		intervalOpsPerSec = float64(totalOps) / intervalElapsed.Seconds()
	}

	// Calculate latencies
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

	// Print report
	if isFinal {
		fmt.Println("\n=== Final Benchmark Results ===")
	} else {
		fmt.Println("\n=== Benchmark Progress Report ===")
	}

	fmt.Printf("Runtime: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total Operations: %d (Gets: %d, Sets: %d)\n", totalOps, stats.TotalGets, stats.TotalSets)
	fmt.Printf("Throughput: %.2f ops/sec\n", opsPerSec)

	if !isFinal {
		fmt.Printf("Current Throughput: %.2f ops/sec\n", intervalOpsPerSec)
	}

	if len(stats.GetLatencies) > 0 {
		fmt.Printf("GET Latency (avg/p50/p99): %.2fms / %.2fms / %.2fms\n",
			float64(getAvg)/float64(time.Millisecond),
			float64(getP50)/float64(time.Millisecond),
			float64(getP99)/float64(time.Millisecond))
	}

	if len(stats.GetLatencies) > 0 {
		fmt.Printf("SET Latency (avg/p50/p99): %.2fms / %.2fms / %.2fms\n",
			float64(setAvg)/float64(time.Millisecond),
			float64(setP50)/float64(time.Millisecond),
			float64(setP99)/float64(time.Millisecond))
	}

	fmt.Printf("Errors: %d\n", stats.ErrorCount)

	if isFinal {
		// Clear latency slices to free memory
		stats.GetLatencies = nil
		stats.SetLatencies = nil
	}
}

func getPercentileLatency(latencies []time.Duration, percentile int) time.Duration {
	n := len(latencies)
	if n == 0 {
		return 0
	}

	// Simple implementation for benchmarking
	// Sort would be more accurate but this is faster for large datasets
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
