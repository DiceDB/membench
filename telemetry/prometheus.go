// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusSink struct {
	PLatencyOp *prometheus.HistogramVec
	PErrorOp   *prometheus.CounterVec
}

func NewPrometheusSink() *PrometheusSink {
	pLatencyOp := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "latency_op_ns_v1",
		Help:    "Observed latencies for an operation in nanoseconds",
		Buckets: prometheus.LinearBuckets(500000, 500000, 20),
	}, []string{"op"})
	prometheus.MustRegister(pLatencyOp)

	pErrorOp := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "error_op_count_v1",
		Help: "Observed errors for an operation",
	}, []string{"op"})
	prometheus.MustRegister(pErrorOp)

	p := &PrometheusSink{
		PLatencyOp: pLatencyOp,
		PErrorOp:   pErrorOp,
	}
	return p
}

func (sink *PrometheusSink) RecordLatencyOpInNanos(latency_ns float64, op string) {
	sink.PLatencyOp.WithLabelValues(op).Observe(latency_ns)
}

func (sink *PrometheusSink) RecordError(op string) {
	sink.PErrorOp.WithLabelValues(op).Inc()
}

func (sink *PrometheusSink) PrintReport() {
	fmt.Println("Prometheus Telemetry Sink. Report not implemented")
}
