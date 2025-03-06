// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
	// Fetch metrics using prometheus API
	fmt.Println("op,latency_ns_avg,latency_ns_p50,latency_ns_p90,latency_ns_p95,latency_ns_p99")
	
	// GET operation metrics
	getLatencies := sink.PLatencyOp.WithLabelValues("GET")
	getMetric := &dto.Metric{}
	getLatencies.(prometheus.Metric).Write(getMetric)
	if getMetric.Histogram != nil {
		fmt.Printf("GET,%v,%v,%v,%v,%v\n",
			calculateMean(getMetric.Histogram),
			calculateQuantile(0.50, getMetric.Histogram),
			calculateQuantile(0.90, getMetric.Histogram),
			calculateQuantile(0.95, getMetric.Histogram),
			calculateQuantile(0.99, getMetric.Histogram))
	}

	// SET operation metrics
	setLatencies := sink.PLatencyOp.WithLabelValues("SET")
	setMetric := &dto.Metric{}
	setLatencies.(prometheus.Metric).Write(setMetric)
	if setMetric.Histogram != nil {
		fmt.Printf("SET,%v,%v,%v,%v,%v\n",
			calculateMean(setMetric.Histogram),
			calculateQuantile(0.50, setMetric.Histogram),
			calculateQuantile(0.90, setMetric.Histogram),
			calculateQuantile(0.95, setMetric.Histogram),
			calculateQuantile(0.99, setMetric.Histogram))
	}

	// Error counts
	fmt.Println("op,error_count")
	getErrors := sink.PErrorOp.WithLabelValues("GET")
	getErrorMetric := &dto.Metric{}
	getErrors.(prometheus.Metric).Write(getErrorMetric)
	if getErrorMetric.Counter != nil {
		fmt.Printf("GET,%v\n", *getErrorMetric.Counter.Value)
	}

	setErrors := sink.PErrorOp.WithLabelValues("SET")
	setErrorMetric := &dto.Metric{}
	setErrors.(prometheus.Metric).Write(setErrorMetric)
	if setErrorMetric.Counter != nil {
		fmt.Printf("SET,%v\n", *setErrorMetric.Counter.Value)
	}
}

// calculateMean calculates the mean from histogram data
func calculateMean(h *dto.Histogram) float64 {
	if h.SampleCount == nil || h.SampleSum == nil || *h.SampleCount == 0 {
		return 0
	}
	return *h.SampleSum / float64(*h.SampleCount)
}

// calculateQuantile estimates the quantile value from histogram buckets
func calculateQuantile(q float64, h *dto.Histogram) float64 {
	if h.SampleCount == nil || *h.SampleCount == 0 {
		return 0
	}

	count := uint64(float64(*h.SampleCount) * q)
	var runningCount uint64
	
	for i, bucket := range h.Bucket {
		runningCount += *bucket.CumulativeCount
		if runningCount >= count {
			// For the first bucket, use the upper bound directly
			if i == 0 {
				return *bucket.UpperBound
			}
			// For other buckets, interpolate between bounds
			lowerBound := *h.Bucket[i-1].UpperBound
			upperBound := *bucket.UpperBound
			return (lowerBound + upperBound) / 2
		}
	}
	
	// If we get here, use the highest bucket's upper bound
	return *h.Bucket[len(h.Bucket)-1].UpperBound
}
