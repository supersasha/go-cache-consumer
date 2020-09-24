BUILD
-----

```
go build src/cmd/cache.go
go build src/cmd/consumer.go
```

RUN
---

See cmd line flags

`config.yml` has additional options:

`ErrorTimeout` -- Time (seconds) after unsuccessful fetch when the URL is marked unreachable in cache.
After that time another attempt to reach the URL is made. Default is 30 seconds.

`RedisHostport` -- Redis host:port
