package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/influxdata/influxdb-observability/jaeger-influxdb/internal"
)

const serviceName = "jaeger-influxdb"

func main() {
	config := new(internal.Config)
	command := &cobra.Command{
		Use:   serviceName,
		Args:  cobra.NoArgs,
		Short: serviceName + " is the Jaeger-InfluxDB gRPC remote storage service",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), config)
		},
	}

	if err := config.Init(command); err != nil {
		fmt.Printf("failed to get config: %s\n", err.Error())
		os.Exit(1)
	}

	logger, err := initLogger(config)
	if err != nil {
		fmt.Printf("failed to start logger: %s\n", err.Error())
		os.Exit(1)
	}

	ctx := contextWithStandardSignals(context.Background())
	ctx = internal.LoggerWithContext(ctx, logger)
	if err := command.ExecuteContext(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Printf("%s\n", err.Error())
			os.Exit(1)
		}
	}
}

func initLogger(config *internal.Config) (*zap.Logger, error) {
	var loggerConfig zap.Config
	if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		loggerConfig = zap.NewDevelopmentConfig()
	} else {
		loggerConfig = zap.NewProductionConfig()
	}
	var err error
	loggerConfig.Level, err = zap.ParseAtomicLevel(config.LogLevel)
	if err != nil {
		return nil, err
	}
	return loggerConfig.Build(zap.AddStacktrace(zap.ErrorLevel))
}

func contextWithStandardSignals(ctx context.Context) context.Context {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			return
		}
	}()
	return ctx
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (ss *contextServerStream) Context() context.Context {
	return ss.ctx
}

func run(ctx context.Context, config *internal.Config) error {
	backend, err := internal.NewInfluxdbStorage(ctx, config)
	if err != nil {
		return err
	}
	defer backend.Close()
	logger := internal.LoggerFromContext(ctx)
	grpcHandler := shared.NewGRPCHandlerWithPlugins(backend, backend, nil)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			res, err := handler(internal.LoggerWithContext(ctx, logger), req)
			if err != nil && err != context.Canceled {
				logger.Error("gRPC interceptor", zap.Error(err))
			}
			return res, err
		}),
		grpc.StreamInterceptor(func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := internal.LoggerWithContext(stream.Context(), logger)
			stream = &contextServerStream{
				ServerStream: stream,
				ctx:          ctx,
			}
			err := handler(srv, stream)
			if err != nil && err != context.Canceled {
				logger.Error("gRPC interceptor", zap.Error(err))
			}
			return err
		}))
	reflection.Register(grpcServer)
	if err = grpcHandler.Register(grpcServer); err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return err
	}
	// grpcServer.Serve() closes this listener, so don't need to close it directly
	defer func() { _ = grpcListener.Close() }()

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errCh <- grpcServer.Serve(grpcListener)
	}()

	internal.LoggerFromContext(ctx).Info("ready")
	<-ctx.Done()
	internal.LoggerFromContext(ctx).Info("exiting")

	grpcServer.GracefulStop()
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		internal.LoggerFromContext(ctx).Warn("the gRPC server is being stubborn, so forcing it to stop")
		grpcServer.Stop()
		select {
		case err = <-errCh:
		case <-time.After(3 * time.Second):
			err = errors.New("the gRPC server never stopped")
		}
	}

	err = multierr.Combine(err, backend.Close())
	return err
}
