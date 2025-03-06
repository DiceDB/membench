// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package reporting

import (
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type MemTelemetrySink struct {
	MemLatencyCommandGET *hdrhistogram.Histogram
	MemLatencyCommandSET *hdrhistogram.Histogram
	MemErrorCommandGET   *hdrhistogram.Histogram
	MemErrorCommandSET   *hdrhistogram.Histogram
}

func NewMemTelemetrySink() *MemTelemetrySink {
	return &MemTelemetrySink{
		MemLatencyCommandGET: hdrhistogram.New(1, 10000, 2),
		MemLatencyCommandSET: hdrhistogram.New(1, 10000, 2),
		MemErrorCommandGET:   hdrhistogram.New(1, 10000, 2),
		MemErrorCommandSET:   hdrhistogram.New(1, 10000, 2),
	}
}

func (sink *MemTelemetrySink) RecordLatencyCommandInNanos(latency_ns float64, command string) {
	if command == "GET" {
		_ = sink.MemLatencyCommandGET.RecordValue(int64(latency_ns))
	} else if command == "SET" {
		_ = sink.MemLatencyCommandSET.RecordValue(int64(latency_ns))
	}
}

func (sink *MemTelemetrySink) RecordError(command string) {
	if command == "GET" {
		_ = sink.MemErrorCommandGET.RecordValue(1)
	} else if command == "SET" {
		_ = sink.MemErrorCommandSET.RecordValue(1)
	}
}

func (sink *MemTelemetrySink) PrintReport() {
	fmt.Println("command,latency_ns_avg,latency_ns_p50,latency_ns_p90,latency_ns_p95,latency_ns_p99")
	fmt.Printf("GET,%v,%v,%v,%v,%v\n",
		int64(sink.MemLatencyCommandGET.Mean()),
		sink.MemLatencyCommandGET.ValueAtQuantile(50),
		sink.MemLatencyCommandGET.ValueAtQuantile(90),
		sink.MemLatencyCommandGET.ValueAtQuantile(95),
		sink.MemLatencyCommandGET.ValueAtQuantile(99))
	fmt.Printf("SET,%v,%v,%v,%v,%v\n",
		int64(sink.MemLatencyCommandSET.Mean()),
		sink.MemLatencyCommandSET.ValueAtQuantile(50),
		sink.MemLatencyCommandSET.ValueAtQuantile(90),
		sink.MemLatencyCommandSET.ValueAtQuantile(95),
		sink.MemLatencyCommandSET.ValueAtQuantile(99))
}
