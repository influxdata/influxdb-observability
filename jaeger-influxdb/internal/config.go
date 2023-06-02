package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/ports"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	LogLevel              string
	ListenAddr            string
	InfluxdbAddr          string
	InfluxdbTLSDisable    bool
	InfluxdbTimeout       time.Duration
	InfluxdbBucket        string
	InfluxdbBucketArchive string
	InfluxdbToken         string
	InfluxdbQueryMetadata map[string]string
}

func (c *Config) Init(command *cobra.Command) error {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	for _, f := range []struct {
		pointer      interface{}
		name         string
		defaultValue interface{}
		usage        string
	}{
		{
			pointer:      &c.LogLevel,
			name:         "log-level",
			defaultValue: zapcore.InfoLevel.String(),
			usage:        "log level (zap)",
		},
		{
			pointer:      &c.ListenAddr,
			name:         "listen-addr",
			defaultValue: fmt.Sprintf(":%d", ports.RemoteStorageGRPC),
			usage:        "Jaeger gRPC storage service (this process) host:port address",
		},
		{
			pointer: &c.InfluxdbAddr,
			name:    "influxdb-addr",
			usage:   "InfluxDB service host:port",
		},
		{
			pointer: &c.InfluxdbTLSDisable,
			name:    "influxdb-tls-disable",
			usage:   "Do not use TLS to connect to InfluxDB (mostly for development)",
		},
		{
			pointer:      &c.InfluxdbTimeout,
			name:         "influxdb-timeout",
			defaultValue: 15 * time.Second,
			usage:        "InfluxDB query timeout",
		},
		{
			pointer: &c.InfluxdbBucket,
			name:    "influxdb-bucket",
			usage:   "InfluxDB bucket name, containing traces, logs, metrics (query only)",
		},
		{
			pointer: &c.InfluxdbBucketArchive,
			name:    "influxdb-bucket-archive",
			usage:   "InfluxDB bucket name, for archiving traces (optional; write and query permissions required)",
		},
		{
			pointer: &c.InfluxdbToken,
			name:    "influxdb-token",
			usage:   "InfluxDB API access token",
		},
		{
			pointer: &c.InfluxdbQueryMetadata,
			name:    "influxdb-query-metadata",
			usage:   `gRPC metadata sent with SQL queries ("foo=bar") (optional; specify zero to many times)`,
		},
	} {
		switch v := f.pointer.(type) {
		case *string:
			var defaultValue string
			if f.defaultValue != nil {
				defaultValue = f.defaultValue.(string)
			}
			command.Flags().StringVar(v, f.name, defaultValue, f.usage)
			if err := viper.BindPFlag(f.name, command.Flags().Lookup(f.name)); err != nil {
				return err
			}
			*v = viper.GetString(f.name)
		case *time.Duration:
			var defaultValue time.Duration
			if f.defaultValue != nil {
				defaultValue = f.defaultValue.(time.Duration)
			}
			command.Flags().DurationVar(v, f.name, defaultValue, f.usage)
			if err := viper.BindPFlag(f.name, command.Flags().Lookup(f.name)); err != nil {
				return err
			}
			*v = viper.GetDuration(f.name)
		case *bool:
			var defaultValue bool
			if f.defaultValue != nil {
				defaultValue = f.defaultValue.(bool)
			}
			command.Flags().BoolVar(v, f.name, defaultValue, f.usage)
			if err := viper.BindPFlag(f.name, command.Flags().Lookup(f.name)); err != nil {
				return err
			}
			*v = viper.GetBool(f.name)
		case *map[string]string:
			var defaultValue map[string]string
			if f.defaultValue != nil {
				defaultValue = f.defaultValue.(map[string]string)
			}
			command.Flags().StringToStringVar(v, f.name, defaultValue, f.usage)
			if err := viper.BindPFlag(f.name, command.Flags().Lookup(f.name)); err != nil {
				return err
			}
			*v = viper.GetStringMapString(f.name)
		default:
			return fmt.Errorf("flag type %T not implemented", f.pointer)
		}
	}
	return nil
}
