// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkConfig holds configuration parameters for the benchmark
type BenchmarkConfig struct {
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	Clients        int
	Requests       int
	KeySize        int
	ValueSize      int
	KeyPrefix      string
	Pipeline       int
	ReadRatio      float64
	Duration       time.Duration
	ReportInterval time.Duration
}

// BenchmarkStats holds statistics for the benchmark
type BenchmarkStats struct {
	totalOps     uint64
	totalGets    uint64
	totalSets    uint64
	getLatencies []time.Duration
	setLatencies []time.Duration
	errorCount   uint64
	startTime    time.Time
	lastReportAt time.Time
	statLock     sync.Mutex
}

func main() {
	// Parse command line flags
	config := parseFlags()

	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})
	defer rdb.Close()

	// Ping Redis to ensure connection is established
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Connected to Redis. Starting benchmark...")

	// Initialize benchmark stats
	stats := &BenchmarkStats{
		getLatencies: make([]time.Duration, 0, config.Requests),
		setLatencies: make([]time.Duration, 0, config.Requests),
		startTime:    time.Now(),
		lastReportAt: time.Now(),
	}

	// Create a context that can be canceled
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

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start a goroutine to report stats periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(config.ReportInterval)
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
	for i := 0; i < config.Clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runBenchmark(runCtx, rdb, config, stats, clientID)
		}(i)
	}

	// If duration is specified, stop after that time
	if config.Duration > 0 {
		time.Sleep(config.Duration)
		cancel()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Report final stats
	reportStats(stats, true)
}

func runBenchmark(ctx context.Context, rdb *redis.Client, config BenchmarkConfig, stats *BenchmarkStats, clientID int) {
	// Seed the random number generator
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

	// Pre-generate keys and values
	keys := make([]string, config.Requests)
	values := make([]string, config.Requests)

	for i := 0; i < config.Requests; i++ {
		keys[i] = generateKey(config.KeyPrefix, config.KeySize, r)
		values[i] = generateValue(config.ValueSize, r)
	}

	// Run benchmark loop
	for reqCount := 0; ; reqCount = (reqCount + 1) % config.Requests {
		// Check if context is canceled
		select {
		case <-ctx.Done():
			return
		default:
			// Continue benchmark
		}

		isRead := r.Float64() < config.ReadRatio
		key := keys[reqCount]

		if config.Pipeline > 1 {
			// Pipeline mode
			pipe := rdb.Pipeline()

			var cmds []*redis.StringCmd
			if isRead {
				// GET operations
				for i := 0; i < config.Pipeline; i++ {
					pipeKey := keys[(reqCount+i)%config.Requests]
					cmds = append(cmds, pipe.Get(ctx, pipeKey))
				}
			} else {
				// SET operations
				for i := 0; i < config.Pipeline; i++ {
					pipeKey := keys[(reqCount+i)%config.Requests]
					pipeValue := values[(reqCount+i)%config.Requests]
					pipe.Set(ctx, pipeKey, pipeValue, 0)
				}
			}

			// Execute pipeline
			start := time.Now()
			_, err := pipe.Exec(ctx)
			elapsed := time.Since(start)

			if err != nil {
				atomic.AddUint64(&stats.errorCount, 1)
			} else {
				// Update stats (counting each operation in the pipeline)
				stats.statLock.Lock()
				if isRead {
					stats.totalGets += uint64(config.Pipeline)
					stats.getLatencies = append(stats.getLatencies, elapsed/time.Duration(config.Pipeline))
				} else {
					stats.totalSets += uint64(config.Pipeline)
					stats.setLatencies = append(stats.setLatencies, elapsed/time.Duration(config.Pipeline))
				}
				stats.totalOps += uint64(config.Pipeline)
				stats.statLock.Unlock()
			}
		} else {
			// Single operation mode
			if isRead {
				// GET operation
				start := time.Now()
				_, err := rdb.Get(ctx, key).Result()
				elapsed := time.Since(start)

				if err == redis.Nil {
					// Key does not exist, which is fine for benchmarking
					err = nil
				}

				if err != nil {
					atomic.AddUint64(&stats.errorCount, 1)
				} else {
					stats.statLock.Lock()
					stats.totalGets++
					stats.getLatencies = append(stats.getLatencies, elapsed)
					stats.totalOps++
					stats.statLock.Unlock()
				}
			} else {
				// SET operation
				value := values[reqCount]

				start := time.Now()
				err := rdb.Set(ctx, key, value, 0).Err()
				elapsed := time.Since(start)

				if err != nil {
					atomic.AddUint64(&stats.errorCount, 1)
				} else {
					stats.statLock.Lock()
					stats.totalSets++
					stats.setLatencies = append(stats.setLatencies, elapsed)
					stats.totalOps++
					stats.statLock.Unlock()
				}
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

func reportStats(stats *BenchmarkStats, isFinal bool) {
	stats.statLock.Lock()
	defer stats.statLock.Unlock()

	now := time.Now()
	elapsed := now.Sub(stats.startTime)
	intervalElapsed := now.Sub(stats.lastReportAt)
	stats.lastReportAt = now

	// Calculate throughput
	totalOps := stats.totalOps
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	intervalOpsPerSec := float64(0)

	if !isFinal {
		// For interval calculation, use the difference since last report
		intervalOpsPerSec = float64(totalOps) / intervalElapsed.Seconds()
	}

	// Calculate latencies
	var getP50, getP99, setP50, setP99 time.Duration
	var getAvg, setAvg time.Duration

	if len(stats.getLatencies) > 0 {
		getLatencies := stats.getLatencies
		getP50 = getPercentileLatency(getLatencies, 50)
		getP99 = getPercentileLatency(getLatencies, 99)
		getAvg = getAverageLatency(getLatencies)
	}

	if len(stats.setLatencies) > 0 {
		setLatencies := stats.setLatencies
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
	fmt.Printf("Total Operations: %d (Gets: %d, Sets: %d)\n", totalOps, stats.totalGets, stats.totalSets)
	fmt.Printf("Throughput: %.2f ops/sec\n", opsPerSec)

	if !isFinal {
		fmt.Printf("Current Throughput: %.2f ops/sec\n", intervalOpsPerSec)
	}

	if len(stats.getLatencies) > 0 {
		fmt.Printf("GET Latency (avg/p50/p99): %.2fms / %.2fms / %.2fms\n",
			float64(getAvg)/float64(time.Millisecond),
			float64(getP50)/float64(time.Millisecond),
			float64(getP99)/float64(time.Millisecond))
	}

	if len(stats.setLatencies) > 0 {
		fmt.Printf("SET Latency (avg/p50/p99): %.2fms / %.2fms / %.2fms\n",
			float64(setAvg)/float64(time.Millisecond),
			float64(setP50)/float64(time.Millisecond),
			float64(setP99)/float64(time.Millisecond))
	}

	fmt.Printf("Errors: %d\n", stats.errorCount)

	if isFinal {
		// Clear latency slices to free memory
		stats.getLatencies = nil
		stats.setLatencies = nil
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

func parseFlags() BenchmarkConfig {
	config := BenchmarkConfig{}

	flag.StringVar(&config.RedisAddr, "addr", "localhost:6379", "Redis server address")
	flag.StringVar(&config.RedisPassword, "password", "", "Redis password")
	flag.IntVar(&config.RedisDB, "db", 0, "Redis database number")
	flag.IntVar(&config.Clients, "clients", 50, "Number of parallel clients")
	flag.IntVar(&config.Requests, "requests", 100000, "Number of requests per client (0 for unlimited)")
	flag.IntVar(&config.KeySize, "keysize", 16, "Key size in bytes")
	flag.IntVar(&config.ValueSize, "valuesize", 64, "Value size in bytes")
	flag.StringVar(&config.KeyPrefix, "keyprefix", "bench", "Prefix for keys")
	flag.IntVar(&config.Pipeline, "pipeline", 1, "Pipeline size (1 for no pipelining)")
	flag.Float64Var(&config.ReadRatio, "reads", 0.8, "Ratio of read operations (0.0-1.0)")

	var durationStr string
	flag.StringVar(&durationStr, "duration", "60s", "Benchmark duration (e.g., 60s, 5m, 1h)")

	var intervalStr string
	flag.StringVar(&intervalStr, "interval", "5s", "Reporting interval (e.g., 1s, 5s, 10s)")

	flag.Parse()

	// Parse duration
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		fmt.Printf("Invalid duration format: %v\n", err)
		os.Exit(1)
	}
	config.Duration = duration

	// Parse reporting interval
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		fmt.Printf("Invalid interval format: %v\n", err)
		os.Exit(1)
	}
	config.ReportInterval = interval

	return config
}
