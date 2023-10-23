// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package main

import (
	"fmt"

	"github.com/spf13/viper"
)

func GetConfig() error {
	configDefaults()

	// Get common configs
	viper.SetConfigName("common")
	viper.AddConfigPath("./conf/")
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Get environment configs
	viper.SetConfigName("env")
	err = viper.MergeInConfig()
	if err != nil {
		return err
	}

	return nil
}

// establishes default values for config
func configDefaults() {
	viper.SetDefault("server.host", "localhost")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.prefix", "rdb")
	viper.SetDefault("redis.password", "")
}

func getRedisAddr() string {
	fmt.Printf("%s:%d\n", viper.Get("redis.host"), viper.Get("redis.port"))
	return fmt.Sprintf("%s:%d", viper.Get("redis.host"), viper.Get("redis.port"))
}

func getServerAddr() string {
	return fmt.Sprintf("%s:%d", viper.Get("server.host"), viper.Get("server.port"))
}
