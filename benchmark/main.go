// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dicedb/membench/config"
	"github.com/dicedb/membench/db"
	"github.com/dicedb/membench/telemetry"
)

func Test(cfg *config.Config) error {
	ctx := context.Background()

	d := newClient(cfg)
	defer d.Close()

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

	var teleSink telemetry.Sink
	if cfg.TelemetrySink == "mem" {
		teleSink = telemetry.NewMemSink()
	} else if cfg.TelemetrySink == "prometheus" {
		teleSink = telemetry.NewPrometheusSink()
	} else {
		panic(fmt.Sprintf("unsupported telemetry sink: %s", cfg.TelemetrySink))
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for range cfg.NumClients {
		wg.Add(1)
		go run(runCtx, cfg, teleSink, &wg)
	}

	wg.Wait()
	teleSink.PrintReport()
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

func run(ctx context.Context, cfg *config.Config, sink telemetry.Sink, wg *sync.WaitGroup) {
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
		var op string

		start := time.Now()
		if isRead {
			op = "GET"
			_, err = d.Get(ctx, key)
		} else {
			op = "SET"
			err = d.Set(ctx, key, value)
		}

		handleOpStats(sink, err, time.Since(start), op)
	}
}

func handleOpStats(sink telemetry.Sink, err error, elapsed time.Duration, op string) {
	if err != nil {
		sink.RecordError(op)
		return
	}
	sink.RecordLatencyOpInNanos(float64(elapsed.Nanoseconds()), op)
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
