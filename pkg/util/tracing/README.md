* start otel-collector
```shell
docker run -p 4320:4320 \
-p 4317:4317 \
-p 8888:8888 \
-p 8889:8889 \
-v /tmp/example-otel-collector-config.yaml:/etc/otel-collector-config.yaml \
otel/opentelemetry-collector:latest \
--config=/etc/otel-collector-config.yaml
```

* start prometheus
```shell
 ./prometheus --config.file=example-prometheus.yml --web.external-url=http://127.0.0.1:9090 --web.listen-address=127.0.0.1:9090 --storage.tsdb.path=data
```
