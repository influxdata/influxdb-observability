package main

import (
	"flag"
	"os"
	"sort"
	"strings"

	"github.com/hashicorp/go-hclog"
	_ "github.com/influxdata/influxdb-iox-client-go"
	"github.com/influxdata/influxdb-observability/jaeger-query-plugin/config"
	"github.com/influxdata/influxdb-observability/jaeger-query-plugin/store"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/spf13/viper"
)

var configPath string

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-query-influxdb",
		Level:      hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		JSONFormat: true,
	})

	flag.StringVar(&configPath, "config", "", "The absolute path to the InfluxDB plugin's configuration file")
	flag.Parse()

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	if configPath != "" {
		v.SetConfigFile(configPath)

		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	conf := config.Configuration{}
	conf.InitFromViper(v)

	environ := os.Environ()
	sort.Strings(environ)
	for _, env := range environ {
		logger.Warn(env)
	}

	logger.Warn("Started with InfluxDB")
	plugin, err := store.NewStore(&conf, logger)

	if err != nil {
		logger.Error("failed to open plugin", "error", err)
		os.Exit(1)
	}

	grpc.Serve(&shared.PluginServices{
		Store: plugin,
	})
}
