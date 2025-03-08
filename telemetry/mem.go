// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type MemSink struct {
	MemLatencyOpGET *hdrhistogram.Histogram
	MemLatencyOpSET *hdrhistogram.Histogram
	MemErrorOpGET   *hdrhistogram.Histogram
	MemErrorOpSET   *hdrhistogram.Histogram
	startTime       time.Time
	totalOps        atomic.Int64
}

func NewMemSink() *MemSink {
	return &MemSink{
		MemLatencyOpGET: hdrhistogram.New(1000, 50000000, 2),
		MemLatencyOpSET: hdrhistogram.New(1000, 50000000, 2),
		MemErrorOpGET:   hdrhistogram.New(1, 10000, 2),
		MemErrorOpSET:   hdrhistogram.New(1, 10000, 2),
		startTime:       time.Now(),
		totalOps:        atomic.Int64{},
	}
}

func (sink *MemSink) RecordLatencyOpInNanos(latency_ns float64, op string) {
	sink.totalOps.Add(1)
	if op == "GET" {
		_ = sink.MemLatencyOpGET.RecordValue(int64(latency_ns))
	} else if op == "SET" {
		_ = sink.MemLatencyOpSET.RecordValue(int64(latency_ns))
	}
}

func (sink *MemSink) RecordError(op string) {
	if op == "GET" {
		_ = sink.MemErrorOpGET.RecordValue(1)
	} else if op == "SET" {
		_ = sink.MemErrorOpSET.RecordValue(1)
	}
}

func (sink *MemSink) PrintReport() {
	fmt.Println("total_ops,elapsed_sec")
	fmt.Printf("%v,%v\n", sink.totalOps.Load(), time.Since(sink.startTime).Seconds())
	fmt.Printf("throughput_ops_per_sec,%v\n", float64(sink.totalOps.Load())/time.Since(sink.startTime).Seconds())
	fmt.Println()

	fmt.Println("Throughput (ops/sec)")
	fmt.Println("GET,SET")
	fmt.Printf("%v,%v\n",
		float64(sink.MemLatencyOpGET.TotalCount())/time.Since(sink.startTime).Seconds(),
		float64(sink.MemLatencyOpSET.TotalCount())/time.Since(sink.startTime).Seconds(),
	)
	fmt.Println()

	fmt.Println("Errors")
	fmt.Println("GET,SET")
	fmt.Printf("%v,%v\n",
		sink.MemErrorOpGET.TotalCount(),
		sink.MemErrorOpSET.TotalCount(),
	)
	fmt.Println()

	fmt.Println("op,latency_ns_avg,latency_ns_p50,latency_ns_p90,latency_ns_p95,latency_ns_p99")
	fmt.Printf("GET,%v,%v,%v,%v,%v\n",
		int64(sink.MemLatencyOpGET.Mean()),
		sink.MemLatencyOpGET.ValueAtQuantile(50),
		sink.MemLatencyOpGET.ValueAtQuantile(90),
		sink.MemLatencyOpGET.ValueAtQuantile(95),
		sink.MemLatencyOpGET.ValueAtQuantile(99))
	fmt.Printf("SET,%v,%v,%v,%v,%v\n",
		int64(sink.MemLatencyOpSET.Mean()),
		sink.MemLatencyOpSET.ValueAtQuantile(50),
		sink.MemLatencyOpSET.ValueAtQuantile(90),
		sink.MemLatencyOpSET.ValueAtQuantile(95),
		sink.MemLatencyOpSET.ValueAtQuantile(99))
}
