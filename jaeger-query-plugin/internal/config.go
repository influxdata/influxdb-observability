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
	LogLevel           string
	ListenAddr         string
	InfluxdbAddr       string
	InfluxdbTimeout    time.Duration
	InfluxdbBucketid   string
	InfluxdbBucketname string
	InfluxdbToken      string
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
			usage:        "Jaeger plugin service (this process) host:port address",
		},
		{
			pointer: &c.InfluxdbAddr,
			name:    "influxdb-addr",
			usage:   "InfluxDB service host:port",
		},
		{
			pointer:      &c.InfluxdbTimeout,
			name:         "influxdb-timeout",
			defaultValue: 30 * time.Second,
			usage:        "InfluxDB query timeout",
		},
		{
			pointer: &c.InfluxdbBucketid,
			name:    "influxdb-bucketid",
			usage:   "InfluxDB bucket containing spans, identified by ID",
		},
		{
			pointer: &c.InfluxdbBucketname,
			name:    "influxdb-bucketname",
			usage:   "InfluxDB bucket containing spans, identified by name",
		},
		{
			pointer: &c.InfluxdbToken,
			name:    "influxdb-token",
			usage:   "InfluxDB API token",
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
		default:
			return fmt.Errorf("flag type %T not implemented", f.pointer)
		}
	}
	return nil
}
