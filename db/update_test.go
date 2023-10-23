// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"testing"
)

func TestUpdateData(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	updateQuery := Query{
		Updates: map[string]string{
			"ea_number": "0",
		},
	}

	validateQuery := Query{
		Filters: []Filter{
			{
				Col: "ea_number",
				Op:  EqualTo,
				Val: []string{"0"},
			},
		}}

	// update all records to ea_number = 0
	err = mr.UpdateData(tableName, updateQuery)
	if err != nil {
		t.Fatalf("Failed updating data %s\n", err)
	}

	tableData, err := mr.GetData(tableName, validateQuery)
	if err != nil {
		t.Fatalf("Failed getting data %s\n", err)
	}
	if len(tableData.Records) != 24 {
		t.Fatalf("Number of records does not match")
	}

	// check to make sure old filter values are deleted and new values are added
	n, err := mr.Client.SCard(Ctx, table.formatFilterKey("ea_number", "111736949")).Result()
	if err != nil {
		t.Fatalf("Failed SCARD of filter set")
	}
	if n != 0 {
		t.Fatal("Old filter value not being removed")
	}

	// checking new filter value is correct
	n, err = mr.Client.SCard(Ctx, table.formatFilterKey("ea_number", "0")).Result()
	if err != nil {
		t.Fatalf("Failed SCARD of filter set")
	}
	if n != 24 {
		t.Fatal("New filter not updated correctly")
	}

	// VERSION 2
	// Reload data and update based with a filter
	tableName, err = mr.reloadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}
	table, err = mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table %s\n", err)
	}

	updateQuery.Filters = []Filter{
		{
			Col: "tokens_allocated",
			Op:  EqualTo,
			Val: []string{"100.0"},
		},
	}

	err = mr.UpdateData(tableName, updateQuery)
	if err != nil {
		t.Fatalf("Failed updating data %s\n", err)
	}

	tableData, err = mr.GetData(tableName, validateQuery)
	if err != nil {
		t.Fatalf("Failed getting data %s\n", err)
	}
	if len(tableData.Records) != 7 {
		t.Fatalf("Number of records does not match")
	}

	// check to make sure old filter values are deleted and new values are added
	n, err = mr.Client.SCard(Ctx, table.formatFilterKey("ea_number", "111736949")).Result()
	if err != nil {
		t.Fatalf("Failed SCARD of filter set")
	}
	if n != 0 {
		t.Fatal("Old filter value not being removed")
	}

	// checking new filter value is correct
	n, err = mr.Client.SCard(Ctx, table.formatFilterKey("ea_number", "0")).Result()
	if err != nil {
		t.Fatalf("Failed SCARD of filter set")
	}
	if n != 7 {
		t.Fatal("New filter value not updated correctly")
	}

	// VERSION 3
	// Reload data and update 2 values ea_number = 0 and tokens_allocated = -1.0
	// Updating values where tokens_allocated <= 100.0
	tableName, err = mr.reloadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}
	table, err = mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table %s\n", err)
	}

	updateQuery.Updates["tokens_allocated"] = "-1.0"

	updateQuery.Filters = []Filter{
		{
			Col: "tokens_allocated",
			Op:  LessThanOrEqual,
			Val: []string{"100.0"},
		},
	}

	err = mr.UpdateData(tableName, updateQuery)
	if err != nil {
		t.Fatalf("Failed updating data %s\n", err)
	}

	validateQuery.Filters = []Filter{
		{
			Col: "tokens_allocated",
			Op:  LessThan,
			Val: []string{"0.0"},
		},
		{
			Col: "ea_number",
			Op:  EqualTo,
			Val: []string{"0"},
		},
	}

	tableData, err = mr.GetData(tableName, validateQuery)
	if err != nil {
		t.Fatalf("Failed getting data %s\n", err)
	}
	if len(tableData.Records) != 13 {
		t.Fatalf("Number of records does not match")
	}

	validateQuery.Filters = []Filter{
		{
			Col: "tokens_allocated",
			Op:  GreaterThanOrEqual,
			Val: []string{"0.0"},
		},
		{
			Col: "tokens_allocated",
			Op:  LessThanOrEqual,
			Val: []string{"100.0"},
		},
	}

	// check to make sure old filter values are deleted and new values are added
	tableData, err = mr.GetData(tableName, validateQuery)
	if err != nil {
		t.Fatalf("Failed getting data %s\n", err)
	}
	if len(tableData.Records) != 0 {
		t.Fatalf("Number of records does not match")
	}
}
