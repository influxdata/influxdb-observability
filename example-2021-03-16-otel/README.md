# OpenTelemetry Collector Example

In this example, HotRod traces (OpenTracing/Jaeger), trace span logs (OpenTracing/Jaeger), and metrics (Prometheus) are delivered to InfluxDB/IOx via OpenTelemetry.

1. `docker-compose up`
2. Browse to HotRod: http://localhost:8000
3. Click a few buttons to generate traces
4. Browse to Jaeger: http://localhost:16686
5. Search for traces
6. View logs with SQL query
```
$ curl --request GET --url 'http://localhost:8080/iox/api/v1/databases/myorg_mybucket/query?format=pretty&q=SELECT%20*%20FROM%20logs'
```
