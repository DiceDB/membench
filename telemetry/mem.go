// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

import (
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type MemSink struct {
	MemLatencyOpGET *hdrhistogram.Histogram
	MemLatencyOpSET *hdrhistogram.Histogram
	MemErrorOpGET   *hdrhistogram.Histogram
	MemErrorOpSET   *hdrhistogram.Histogram
}

func NewMemSink() *MemSink {
	return &MemSink{
		MemLatencyOpGET: hdrhistogram.New(1000, 50000000, 2),
		MemLatencyOpSET: hdrhistogram.New(1000, 50000000, 2),
		MemErrorOpGET:   hdrhistogram.New(1, 10000, 2),
		MemErrorOpSET:   hdrhistogram.New(1, 10000, 2),
	}
}

func (sink *MemSink) RecordLatencyOpInNanos(latency_ns float64, op string) {
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

	fmt.Println("op,error_count")
	fmt.Printf("GET,%v\n", sink.MemErrorOpGET.TotalCount())
	fmt.Printf("SET,%v\n", sink.MemErrorOpSET.TotalCount())
}
