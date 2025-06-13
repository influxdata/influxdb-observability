package internal

import (
	"context"

	"go.uber.org/zap"
)

type loggerContext struct{}

func LoggerWithContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerContext{}, logger)
}

func LoggerFromContext(ctx context.Context) *zap.Logger {
	logger, _ := ctx.Value(loggerContext{}).(*zap.Logger)
	return logger
}
