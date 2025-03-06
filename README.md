membench
===

Membench is the tool to benchmark various in-mem databases for various operations.

1. make sure you have a running instance of Redis

```
$ docker start redis-server || docker run --name redis-server -d -p 6379:6379 redis
```

2. execute the following comamnd

```
$ make run-redis
```

## Reporting

```
$ ip route | sed -n '2p' | awk '{print $NF}'
```

By default the metrics will be printed on stdout, but if you want to
also emit the metrics to some sink like prometheus, pass the following
flag

```
$ ... --emit-metrics-sink prometheus
```

To run Prometheus locally using Docker, run the following command. It starts
Prometheus and Grafana.

```
$ docker-compose up
```

## Deleting Prometheus Data

```
$ docker exec -it prometheus sh -c "rm -rf /prometheus/*"
$ docker restart prometheus
```
