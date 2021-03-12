package otel2lineprotocol

type Logger interface {
	Debug(msg string, kv ...interface{})
}

type NoopLogger struct{}

func (_ NoopLogger) Debug(_ string, _ ...interface{}) {}

type errorLogger struct {
	Logger
}

func (e *errorLogger) Debug(msg string, kv ...interface{}) {
	for i := range kv {
		if _, isError := kv[i].(error); isError {
			kv = append(kv[:i], nil)
			copy(kv[i+1:], kv[i:])
			kv[i] = "error"
		}
	}
	e.Logger.Debug(msg, kv...)
}
