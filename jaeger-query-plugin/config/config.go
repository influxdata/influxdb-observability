package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	influxDBPrefix = "influxdb."

	flagHost     = influxDBPrefix + "host"
	flagDatabase = influxDBPrefix + "database"
	flagTimeout  = influxDBPrefix + "timeout"
)

// Configuration describes the options to customize the storage behavior
type Configuration struct {
	Host     string        `yaml:"host"`
	Database string        `yaml:"database"`
	Timeout  time.Duration `yaml:"timeout"`
}

// InitFromViper initializes the options struct with values from Viper
func (c *Configuration) InitFromViper(v *viper.Viper) {
	c.Host = v.GetString(flagHost)
	c.Database = v.GetString(flagDatabase)
	v.SetDefault(flagTimeout, "10s")
	c.Timeout = v.GetDuration(flagTimeout)
}
