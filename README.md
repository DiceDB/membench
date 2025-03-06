membench
===

Membench is the tool to benchmark in-mem databases like
DiceDB, Redis, etc for various operations.

To run `membench`, build the membench from source

```
$ make build
```

## Running Membench

```
$ ./membench benchmark --database dicedb \
    --host localhost \
    --port 7379 \
    --num-requests 100000 \
    --num-clients 4
```

You can always get help by running

```
$ ./membench benchmark --help
```

## Telemetry Sink

Membench supports multiple telemetry sinks like

- `mem` - which accumulates the stats in an in-mem
- `prometheus` - which emits the stats to a prometheus instance

You can configure this using the flag `--telemetry-sink`.

### Memory - Telemetry Sink

Accumulates all the metrics in in-memory histograms and outputs
the report at the end of the benchmark.

```
command,latency_ns_avg,latency_ns_p50,latency_ns_p90,latency_ns_p95,latency_ns_p99
GET,82647,71679,103935,130047,264191
SET,89865,73215,107007,134143,290815
command,error_count
GET,0
SET,0
```

### Prometheus - Telemetry Sink

To make it simpler, there is a `docker-compose.yml` file that
starts `prometheus` and `grafana`. You can run the following command
to start the telemetry stack

```
$ docker compose up
```

If you are using `prometheus`, then make sure you are updating the file
`prometheus.yml` and setting the correct IP address.

You can get the IP address of the machine using the following command

```
$ ip route | sed -n '2p' | awk '{print $NF}'
```

You can also use `grafana.json` file and load it to visualize the
membench as it runs with all key vitals.
