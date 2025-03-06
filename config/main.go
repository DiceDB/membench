// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var C *Config

type Config struct {
	Host string `mapstructure:"host" default:"localhost"`
	Port int    `mapstructure:"port" default:"7379"`

	Database string `mapstructure:"database" default:"dicedb"`

	NumClients  int     `mapstructure:"num-clients" default:"50"`
	NumRequests int     `mapstructure:"num-requests" default:"100000"`
	KeySize     int     `mapstructure:"key-size" default:"16"`
	ValueSize   int     `mapstructure:"value-size" default:"64"`
	KeyPrefix   string  `mapstructure:"key-prefix" default:"mb-"`
	ReadRatio   float64 `mapstructure:"read-ratio" default:"0.8"`

	TelemetrySink string `mapstructure:"telemetry-sink" default:"mem"`
}

func Init(flags *pflag.FlagSet) {
	_ = viper.BindPFlags(flags)
	C = &Config{}
	if err := viper.Unmarshal(C); err != nil {
		panic(err)
	}
}
