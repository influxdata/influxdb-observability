# OpenTelemetry Collector Example

In this example, HotRod traces (OpenTracing/Jaeger) and trace span logs (OpenTracing/Jaeger) are delivered to OpenTelemetry Collector via Jaeger Thrift UDP, then to Telegraf via OTLP gRPC, then to InfluxDB/IOx via line protocol.

1. `docker-compose up`
2. Browse to HotRod: http://localhost:8000
3. Click a few buttons to generate traces
4. Browse to Jaeger: http://localhost:16686
5. Search for traces
6. View logs with SQL query
```
$ curl --request GET --url 'http://localhost:8080/iox/api/v1/databases/myorg_mybucket/query?format=pretty&q=SELECT%20*%20FROM%20logs'
```
