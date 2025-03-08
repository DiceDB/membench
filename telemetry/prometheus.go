// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

import (
    "context"
    "fmt"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/api"
    v1 "github.com/prometheus/client_golang/api/prometheus/v1"
    "github.com/prometheus/common/model"
)

type PrometheusSink struct {
	PLatencyOp *prometheus.HistogramVec
	PErrorOp   *prometheus.CounterVec
	apiClient  v1.API
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

	client, err := api.NewClient(api.Config{
		Address: "http://localhost:9090", // Prometheus server address
	})
	if err != nil {
		panic(fmt.Sprintf("Error creating Prometheus client: %v", err))
	}

	p := &PrometheusSink{
		PLatencyOp: pLatencyOp,
		PErrorOp:   pErrorOp,
		apiClient:  v1.NewAPI(client),
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
    ctx := context.Background()
    now := time.Now()

    fmt.Println("op,latency_ns_avg,latency_ns_p50,latency_ns_p90,latency_ns_p95,latency_ns_p99")

    // Query latency metrics for GET operations
    getAvg, _, err := sink.apiClient.Query(ctx, `avg(latency_op_ns_v1{op="GET"})`, now)
    if err != nil {
        fmt.Printf("Error querying GET avg: %v\n", err)
    }
    getP50, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.50, sum(rate(latency_op_ns_v1_bucket{op="GET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying GET p50: %v\n", err)
    }
    getP90, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.90, sum(rate(latency_op_ns_v1_bucket{op="GET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying GET p90: %v\n", err)
    }
    getP95, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.95, sum(rate(latency_op_ns_v1_bucket{op="GET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying GET p95: %v\n", err)
    }
    getP99, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.99, sum(rate(latency_op_ns_v1_bucket{op="GET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying GET p99: %v\n", err)
    }

    fmt.Printf("GET,%v,%v,%v,%v,%v\n",
        getScalarValue(getAvg),
        getScalarValue(getP50),
        getScalarValue(getP90),
        getScalarValue(getP95),
        getScalarValue(getP99))

    // Query latency metrics for SET operations
    setAvg, _, err := sink.apiClient.Query(ctx, `avg(latency_op_ns_v1{op="SET"})`, now)
    if err != nil {
        fmt.Printf("Error querying SET avg: %v\n", err)
    }
    setP50, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.50, sum(rate(latency_op_ns_v1_bucket{op="SET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying SET p50: %v\n", err)
    }
    setP90, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.90, sum(rate(latency_op_ns_v1_bucket{op="SET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying SET p90: %v\n", err)
    }
    setP95, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.95, sum(rate(latency_op_ns_v1_bucket{op="SET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying SET p95: %v\n", err)
    }
    setP99, _, err := sink.apiClient.Query(ctx, `histogram_quantile(0.99, sum(rate(latency_op_ns_v1_bucket{op="SET"}[5m])) by (le))`, now)
    if err != nil {
        fmt.Printf("Error querying SET p99: %v\n", err)
    }

    fmt.Printf("SET,%v,%v,%v,%v,%v\n",
        getScalarValue(setAvg),
        getScalarValue(setP50),
        getScalarValue(setP90),
        getScalarValue(setP95),
        getScalarValue(setP99))

    // Query error counts
    fmt.Println("op,error_count")
    getErrors, _, err := sink.apiClient.Query(ctx, `sum(error_op_count_v1{op="GET"})`, now)
    if err != nil {
        fmt.Printf("Error querying GET errors: %v\n", err)
    }
    setErrors, _, err := sink.apiClient.Query(ctx, `sum(error_op_count_v1{op="SET"})`, now)
    if err != nil {
        fmt.Printf("Error querying SET errors: %v\n", err)
    }

    fmt.Printf("GET,%v\n", getScalarValue(getErrors))
    fmt.Printf("SET,%v\n", getScalarValue(setErrors))
}

// Helper function to extract scalar values from Prometheus query results
func getScalarValue(result model.Value) float64 {
    if result == nil {
        return 0
    }
    switch v := result.(type) {
    case *model.Scalar:
        return float64(v.Value)
    case model.Vector:
        if len(v) > 0 {
            return float64(v[0].Value)
        }
    }
    return 0
}
