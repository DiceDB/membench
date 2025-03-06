// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package benchmark

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunPrometheusHTTPMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe("0.0.0.0:8080", nil))
}
