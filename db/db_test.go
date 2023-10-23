// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var (
	testSchema1 = Schema{
		Name: "table1",
		Columns: []Column{
			{
				Name:       "col1",
				DataType:   "int",
				Filterable: true,
				Sortable:   false,
			},
			{
				Name:       "col2",
				DataType:   "string",
				Filterable: false,
				Sortable:   false,
			},
			{
				Name:       "col3",
				DataType:   "int",
				Filterable: true,
				Sortable:   true,
			},
		},
	}
	testSchema2 = Schema{
		Name: "table2",
		Columns: []Column{
			{
				Name:       "col1",
				DataType:   "string",
				Filterable: false,
				Sortable:   false,
			},
			{
				Name:       "col2",
				DataType:   "int",
				Filterable: true,
				Sortable:   true,
			},
			{
				Name:       "col3",
				DataType:   "bool",
				Filterable: true,
				Sortable:   false,
			},
		},
	}
)

func newMiniRedis(t *testing.T) *Database {
	// https: //github.com/alicebob/miniredis
	mr := miniredis.RunT(t)

	// https: //elliotchance.medium.com/mocking-redis-in-unit-tests-in-go-28aff285b98
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Checks to make sure client is alive
	err := client.Ping(Ctx).Err()
	if err != nil {
		t.Fatal("Failed to ping miniredis")
	}

	return &Database{
		Client: client,
	}
}
