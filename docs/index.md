# InfluxDB/IOx Common Observability Schema

*Perfect is the enemy of good.*

Reference for InfluxDB/IOx schema, in terms of the OpenTelemetry data model.
The goal of this schema is to be (1) a common reference for clients writing to and reading from InfluxDB/IOx and (2) a common reference for humans performing ad-hoc queries to troubleshoot observed systems.

While OpenTelemetry is the primary reference, translation to/from some other common schemas are also provided.

InfluxDB value types are expressed as tag and field.
Tags and fields have non-empty string keys.
Tags have string values, and fields have basic scalar values: string, int, uint, float, bool.

Non-finite floating-point field values (+/- infinity and NaN from IEEE 754) are not currently supported by InfluxDB/IOx, but are part of the design spec.
Therefore, no special consideration is given here.

## Signal Types

- [Traces](traces.md)
- [Metrics](metrics.md)
- [Logs](logs.md)
