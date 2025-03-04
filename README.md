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
