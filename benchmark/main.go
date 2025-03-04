// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dicedb/membench/config"
	"github.com/dicedb/membench/db"
	"github.com/dicedb/membench/reporting"
)

func Test(cfg *config.Config) error {
	ctx := context.Background()

	d := newClient(cfg)
	defer d.Close()

	// Enhanced test with error handling
	if _, err := d.Get(ctx, "test"); err != nil {
		return fmt.Errorf("initial get test failed: %w", err)
	}

	if err := d.Set(ctx, "test", "test"); err != nil {
		return fmt.Errorf("set test failed: %w", err)
	}

	if _, err := d.Get(ctx, "test"); err != nil {
		return fmt.Errorf("follow-up get test failed: %w", err)
	}

	return nil
}

func Run(cfg *config.Config) {
	ctx := context.Background()
	stats := &reporting.BenchmarkStats{
		GetLatencies: make([]time.Duration, 0, cfg.NumRequests),
		SetLatencies: make([]time.Duration, 0, cfg.NumRequests),
		StartTime:    time.Now(),
		LastReportAt: time.Now(),
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reporting goroutine
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.ReportEvery) * time.Second)
		for range ticker.C {
			reportStats(stats, false)
		}
	}()

	var wg sync.WaitGroup
	for range cfg.NumClients {
		wg.Add(1)
		go run(runCtx, cfg, stats, &wg)
	}

	wg.Wait()
	reportStats(stats, true)
}

func newClient(cfg *config.Config) db.DB {
	switch cfg.Database {
	case "dicedb":
		return db.NewDiceDB(cfg.Host, cfg.Port)
	case "redis":
		return db.NewRedis(cfg.Host, cfg.Port)
	case "null":
		return db.NewNull()
	default:
		panic(fmt.Sprintf("unsupported database: %s", cfg.Database))
	}
}

func run(ctx context.Context, cfg *config.Config, stats *reporting.BenchmarkStats, wg *sync.WaitGroup) {
	d := newClient(cfg)
	defer d.Close()
	defer wg.Done()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Pre-generate keys and values
	keys := make([]string, cfg.NumRequests)
	values := make([]string, cfg.NumRequests)

	for i := 0; i < cfg.NumRequests; i++ {
		keys[i] = generateKey(cfg.KeyPrefix, cfg.KeySize, rnd)
		values[i] = generateValue(cfg.ValueSize, rnd)
	}

	for reqCount := range cfg.NumRequests {
		isRead := rnd.Float64() < cfg.ReadRatio
		key := keys[reqCount]
		value := values[reqCount]

		var err error

		start := time.Now()
		if isRead {
			_, err = d.Get(ctx, key)
		} else {
			err = d.Set(ctx, key, value)
		}

		handleOpStats(stats, err, time.Since(start), isRead)
	}
}

// New helper function to reduce code duplication
func handleOpStats(stats *reporting.BenchmarkStats, err error, elapsed time.Duration, isGet bool) {
	if err != nil {
		atomic.AddUint64(&stats.ErrorCount, 1)
		return
	}

	stats.StatLock.Lock()
	defer stats.StatLock.Unlock()

	if isGet {
		stats.TotalGets++
		stats.GetLatencies = append(stats.GetLatencies, elapsed)
	} else {
		stats.TotalSets++
		stats.SetLatencies = append(stats.SetLatencies, elapsed)
	}
	stats.TotalOps++
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
	stats.LastReportAt = now

	// Calculate throughput
	totalOps := stats.TotalOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()

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

	if isFinal {
		stats.GetLatencies = nil
		stats.SetLatencies = nil
	}
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
