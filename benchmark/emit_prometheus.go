package benchmark

import (
	"log"
	"net/http"

	"github.com/dicedb/membench/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunPrometheusHTTPMetricsServer() {
	prometheus.MustRegister(reporting.PMetricsLatencyGet)
	prometheus.MustRegister(reporting.PMetricsLatencySet)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe("0.0.0.0:8080", nil))
}
