// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package cmd

import (
	"fmt"
	"log"
	"sync"

	"github.com/dicedb/membench/benchmark"
	"github.com/dicedb/membench/config"
	"github.com/spf13/cobra"
)

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Runs the benchmark with the given configuration",
	Run: func(cmd *cobra.Command, args []string) {
		config.Init(cmd.Flags())
		err := benchmark.Test(config.C)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("all things checked ... database is ready")
		fmt.Println("running benchmark ...")

		var rwg sync.WaitGroup
		rwg.Add(1)
		go benchmark.Run(config.C, &rwg)

		if config.C.TelemetrySink == "prometheus" {
			go benchmark.RunPrometheusHTTPMetricsServer()
		}

		rwg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(benchmarkCmd)

	benchmarkCmd.Flags().String("host", "localhost", "database host address")
	benchmarkCmd.Flags().Int("port", 7379, "database port")

	benchmarkCmd.Flags().String("database", "dicedb", "database to benchmark (dicedb, redis, null)")

	benchmarkCmd.Flags().Int("num-clients", 50, "number of parallel clients to simulate")
	benchmarkCmd.Flags().Int("num-requests", 100000, "number of requests to hit per simulated client")
	benchmarkCmd.Flags().Int("key-size", 16, "key size in bytes")
	benchmarkCmd.Flags().Int("value-size", 64, "value size in bytes")
	benchmarkCmd.Flags().String("key-prefix", "mb", "prefix for keys")
	benchmarkCmd.Flags().Float64("read-ratio", 0.8, "ratio of read to write operations (0.0-1.0)")

	benchmarkCmd.Flags().String("telemetry-sink", "mem", "sink for the benchmark telemetry (mem, prometheus)")
}
