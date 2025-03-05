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

func Run(cfg *config.Config, rwg *sync.WaitGroup) {
	defer rwg.Done()

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
			stats.Print()
		}
	}()

	var wg sync.WaitGroup
	for range cfg.NumClients {
		wg.Add(1)
		go run(runCtx, cfg, stats, &wg)
	}

	wg.Wait()
	stats.Print()
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

		handleOpStats(cfg, stats, err, time.Since(start), isRead)
	}
}

// New helper function to reduce code duplication
func handleOpStats(cfg *config.Config, stats *reporting.BenchmarkStats, err error, elapsed time.Duration, isGet bool) {
	if err != nil {
		atomic.AddUint64(&stats.ErrorCount, 1)
		return
	}

	stats.StatLock.Lock()
	defer stats.StatLock.Unlock()

	if isGet {
		stats.TotalGets++
		stats.GetLatencies = append(stats.GetLatencies, elapsed)
		if cfg.EmitMetricsSink == "prometheus" {
			stats.EmitGet(float64(elapsed.Nanoseconds()))
		}
	} else {
		stats.TotalSets++
		stats.SetLatencies = append(stats.SetLatencies, elapsed)
		if cfg.EmitMetricsSink == "prometheus" {
			stats.EmitSet(float64(elapsed.Nanoseconds()))
		}
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
