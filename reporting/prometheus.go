// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package reporting

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusTelemetrySink struct {
	PLatencyCommand *prometheus.HistogramVec
	PErrorCommand   *prometheus.CounterVec
}

func NewPrometheusTelemetrySink() *PrometheusTelemetrySink {
	pLatencyCommand := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "latency_command_ns_v1",
		Help:    "Observed latencies for a command in nanoseconds",
		Buckets: prometheus.LinearBuckets(500000, 500000, 20),
	}, []string{"command"})
	prometheus.MustRegister(pLatencyCommand)

	pErrorCommand := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "error_command_count_v1",
		Help: "Observed errors for a command",
	}, []string{"command"})
	prometheus.MustRegister(pErrorCommand)

	p := &PrometheusTelemetrySink{
		PLatencyCommand: pLatencyCommand,
		PErrorCommand:   pErrorCommand,
	}
	return p
}

func (sink *PrometheusTelemetrySink) RecordLatencyCommandInNanos(latency_ns float64, command string) {
	sink.PLatencyCommand.WithLabelValues(command).Observe(latency_ns)
}

func (sink *PrometheusTelemetrySink) RecordError(command string) {
	sink.PErrorCommand.WithLabelValues(command).Inc()
}

func (sink *PrometheusTelemetrySink) PrintReport() {
	fmt.Println("Prometheus Telemetry Sink. Report not implemented")
}
