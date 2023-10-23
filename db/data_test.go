// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGetAllRecordKeys(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	// keys, _, err := mr.getAllRecordKeys(table, 0, -1, 0, "")
	keys, _, err := mr.getRecordKeys(table, Query{})
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 24 {
		t.Fatalf("Expected 24 record keys, got %d\n", len(*keys))
	}
}

func TestGetFilteredRecordKeys(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	// First with 1 filter and 1 value
	query := Query{
		Filters: []Filter{
			{
				Col: "col3_string",
				Op:  EqualTo,
				Val: []string{"AMER"},
			},
		}}
	keys, _, err := mr.getRecordKeys(table, query)
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 11 {
		t.Fatalf("Expected 11 record keys, got %d\n", len(*keys))
	}

	//  1 filter and 2 values
	query.Filters[0].Val = append(query.Filters[0].Val, "EMEA")
	keys, _, err = mr.getRecordKeys(table, query)
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 19 {
		t.Fatalf("Expected 19 record keys, got %d\n", len(*keys))
	}

	//  2 filter and 2 values
	query.Filters = append(query.Filters, Filter{
		Col: "col4_int",
		Op:  EqualTo,
		Val: []string{"0.0"},
	})
	keys, _, err = mr.getRecordKeys(table, query)
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 5 {
		t.Fatalf("Expected 5 record keys, got %d\n", len(*keys))
	}

	// filter with no records
	query.Filters[1].Val = []string{"blah"}
	keys, _, err = mr.getRecordKeys(table, query)
	if err != ErrNil {
		t.Fatal("Not returning ErrNil for no records")
	}
	// if len(*keys) != 0 {
	// 	t.Fatalf("Expected 0 record keys, got %d\n", len(*keys))
	// }

	// filter column that DNE
	// filters["blah"] = []string{"blah"}
	query.Filters = append(query.Filters, Filter{
		Col: "blah",
		Op:  EqualTo,
		Val: []string{"blah"},
	})
	keys, _, err = mr.getRecordKeys(table, query)
	if err == nil {
		t.Fatalf("Not failing for filtering a column that does not exist")
	}
	// if len(*keys) != 0 {
	// 	t.Fatalf("Expected 0 record keys, got %d\n", len(*keys))
	// }
}

func TestGetRecordKeys(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	// ensure empty filter returns 24 records
	query := Query{}
	keys, _, err := mr.getRecordKeys(table, query)
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 24 {
		t.Fatalf("Expected 0 record keys, got %d\n", len(*keys))
	}

	// Add some filters and check
	query.Filters = append(query.Filters, Filter{
		Col: "col3_string",
		Op:  EqualTo,
		Val: []string{"APAC", "EMEA"},
	})
	query.Filters = append(query.Filters, Filter{
		Col: "col4_int",
		Op:  EqualTo,
		Val: []string{"0.0", "100.0"},
	})
	keys, _, err = mr.getRecordKeys(table, query)
	if err != nil {
		t.Fatalf("Failed getting all record keys %s\n", err)
	}
	if len(*keys) != 5 {
		t.Fatalf("Expected 5 record keys, got %d\n", len(*keys))
	}
}

func TestGetRecord(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	// test record matches
	key12 := table.formatRecordKey(12)
	recordTest := map[string]string{
		"col2_string": "company12",
		"col1_int":    "111155042",
		"col3_string": "AMER",
		"col4_int":    "0.0",
	}
	record, err := mr.getRecord(key12, &table.Schema)
	if err != nil {
		t.Fatalf("Failed getting record%s\n", err)
	}
	if !reflect.DeepEqual(recordTest, *record) {
		t.Fatalf("record is not matching expected\n")
	}

	// Test failing on column not in schema
	mr.Client.HSet(Ctx, key12, "blah", "blah")
	record, err = mr.getRecord(key12, &table.Schema)
	if err == nil {
		t.Fatalf("Did not fail getting record with missing column%s\n", err)
	}

	// Test failure on missing column
	mr.Client.HDel(Ctx, key12, "blah")
	mr.Client.HDel(Ctx, key12, "col2_string")
	record, err = mr.getRecord(key12, &table.Schema)
	if err == nil {
		t.Fatalf("Did not fail getting record with missing column%s\n", err)
	}
}

func TestGetAllDataSmall(t *testing.T) {
	mr := newMiniRedis(t)

	table, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	tableData, err := mr.GetData(table, Query{})
	if err != nil {
		t.Fatalf("Failed getting all data %s\n", err)
	}
	if len(tableData.Records) != 24 {
		t.Fatalf("Number of records does not match")
	}

}

func TestGetSortedDataErrors(t *testing.T) {
	mr := newMiniRedis(t)

	table, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	// sorting a non-sortable column
	query := Query{
		Filters: []Filter{{
			Col: "col2_string",
			Op:  LessThanOrEqual,
			Val: []string{"blah"},
		},
		}}
	_, err = mr.GetData(table, query)
	if err == nil {
		t.Fatalf("Not failing for sorting on a non-sortable column")
	}

	// sorting a non existant column
	query.Filters = append(query.Filters, Filter{
		Col: "blah",
		Op:  LessThanOrEqual,
		Val: []string{"blah"},
	})
	_, err = mr.GetData(table, query)
	if err == nil {
		t.Fatalf("Not failing for sorting on a non existing column")
	}

	// sorting a non float val
	query.Filters = append(query.Filters, Filter{
		Col: "col1_int",
		Op:  LessThanOrEqual,
		Val: []string{"blah"},
	})
	_, err = mr.GetData(table, query)
	if err == nil {
		t.Fatalf("Not failing for sorting on a value that is not a float")
	}
}

func TestGetSortedData(t *testing.T) {
	mr := newMiniRedis(t)

	table, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	query := Query{}

	// LTE
	query.Filters = append(query.Filters, Filter{
		Col: "col4_int",
		Op:  LessThanOrEqual,
		Val: []string{"100.0"},
	})
	tableData, err := mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 13 {
		t.Fatalf("Number of records does not match")
	}

	// LT
	query.Filters[0].Op = LessThan
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lt data %s\n", err)
	}
	if len(tableData.Records) != 6 {
		t.Fatalf("Number of records does not match")
	}

	// GTE
	query.Filters[0].Op = GreaterThan
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting gt data %s\n", err)
	}
	if len(tableData.Records) != 11 {
		t.Fatalf("Number of records does not match")
	}

	// GTE
	query.Filters[0].Op = GreaterThanOrEqual
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting gte data %s\n", err)
	}
	if len(tableData.Records) != 18 {
		t.Fatalf("Number of records does not match")
	}

	// Two Filters on same column
	query.Filters = append(query.Filters, Filter{
		Col: "col4_int",
		Op:  LessThan,
		Val: []string{"20000.0"},
	})
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting gte data %s\n", err)
	}
	if len(tableData.Records) != 11 {
		t.Fatalf("Number of records does not match")
	}

	// Try adding one additional non sortable filter
	query.Filters = append(query.Filters, Filter{
		Col: "col2_string",
		Op:  EqualTo,
		Val: []string{"company17"},
	})
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting data %s\n", err)
	}
	if len(tableData.Records) != 1 {
		t.Fatalf("Number of records does not match")
	}
}

func TestGetAllDataPaging(t *testing.T) {
	mr := newMiniRedis(t)

	table, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading xpp test data %s\n", err)
	}

	query := Query{}

	tableData, err := mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 24 {
		t.Fatalf("Number of records does not match")
	}

	// Check a few records to verify order
	if tableData.Records[5]["col2_string"] != "company6" {
		t.Fatalf(fmt.Sprintf("Record 5 col2_string is %s, expected company6", tableData.Records[5]["col2_string"]))
	}
	if tableData.Records[12]["col2_string"] != "company12" {
		t.Fatalf(fmt.Sprintf("Record 12 col2_string is %s, expected company12", tableData.Records[5]["col2_string"]))
	}
	if tableData.Records[23]["col2_string"] != "company23" {
		t.Fatalf(fmt.Sprintf("Record 23 col2_string is %s, expected company23", tableData.Records[5]["col2_string"]))
	}

	// Test limit
	query.Limit = 10
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 10 {
		t.Fatalf("Number of records does not match")
	}

	// Check a few records to verify order
	if tableData.Records[5]["col2_string"] != "company6" {
		t.Fatalf(fmt.Sprintf("Record 5 col2_string is %s, expected company6", tableData.Records[5]["col2_string"]))
	}
	if tableData.Records[9]["col2_string"] != "company5" {
		t.Fatalf(fmt.Sprintf("Record 9 col2_string is %s, expected company5", tableData.Records[5]["col2_string"]))
	}

	// Test offset
	query.Offset = 10
	query.Limit = 0
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 14 {
		t.Fatalf("Number of records does not match")
	}

	// Check a few records to verify order
	if tableData.Records[0]["col2_string"] != "company10" {
		t.Fatalf(fmt.Sprintf("Record 0 col2_string is %s, expected company10", tableData.Records[5]["col2_string"]))
	}
	if tableData.Records[8]["col2_string"] != "company18" {
		t.Fatalf(fmt.Sprintf("Record 8 col2_string is %s, expected company18", tableData.Records[5]["col2_string"]))
	}

	// Test limit and offset
	query.Limit = 7
	query.Offset = 4
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 7 {
		t.Fatalf("Number of records does not match")
	}

	// Check a few records to verify order
	if tableData.Records[0]["col2_string"] != "company5" {
		t.Fatalf(fmt.Sprintf("Record 0 col2_string is %s, expected company5", tableData.Records[5]["col2_string"]))
	}
	if tableData.Records[3]["col2_string"] != "company8" {
		t.Fatalf(fmt.Sprintf("Record 3 col2_string is %s, expected company8", tableData.Records[5]["col2_string"]))
	}

	// Test limit goes past last record
	query.Limit = 10
	query.Offset = 23
	tableData, err = mr.GetData(table, query)
	if err != nil {
		t.Fatalf("Failed getting lte data %s\n", err)
	}
	if len(tableData.Records) != 1 {
		t.Fatalf("Number of records does not match")
	}
}
