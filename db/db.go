// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type Database struct {
	Client *redis.Client
}

const (
	MaxWorkers = 8
)

var (
	ErrNil          = errors.New("no matching records found in redis database")
	ErrImmutableKey = errors.New("updating immutable key")
	ErrEmptyKey     = errors.New("empty key")

	Ctx = context.TODO()

	Prefix = "rdb"
)

// Connects to the given redis address and creates a new client
// returns a new Database object
func NewDatabase(address string, prefix string, password string) (*Database, error) {
	Prefix = prefix

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})

	// Checks to make sure client is alive
	err := client.Ping(Ctx).Err()
	if err != nil {
		return nil, err
	}

	return &Database{
		Client: client,
	}, nil
}
