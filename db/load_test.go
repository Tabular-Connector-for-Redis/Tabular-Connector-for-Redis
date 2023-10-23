// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

// Loads small data and schema for table1 table
func (mr *Database) loadXPPTestData() (string, error) {
	table := "table1"
	f, err := os.Open("testData/test_data_small.csv")
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Read xpp schema
	j, err := os.Open("testData/test_schema.json")
	if err != nil {
		return "", err
	}
	defer j.Close()
	jsonData, err := ioutil.ReadAll(j)
	if err != nil {
		return "", err
	}
	var schema Schema
	json.Unmarshal(jsonData, &schema)

	// Add xpp schema
	err = mr.AddSchema(&schema)
	if err != nil {
		return "", err
	}
	// run bulk load successfully
	err = mr.BulkLoad(table, f, "csv")
	if err != nil {
		return "", err
	}

	return table, nil
}

// reloads test data without adding schema
func (mr *Database) reloadXPPTestData() (string, error) {
	table := "table1"
	f, err := os.Open("testData/test_data_small.csv")
	if err != nil {
		return "", err
	}
	defer f.Close()

	// run bulk load successfully
	err = mr.BulkLoad(table, f, "csv")
	if err != nil {
		return "", err
	}

	return table, nil
}

func TestGetLastLoad(t *testing.T) {
	mr := newMiniRedis(t)
	table := Table{Name: "table1"}

	// get last load for a table that DNE
	load, err := mr.GetLastLoad(table)
	if err != ErrNil {
		t.Fatalf("GetLastLoad did not return ErrNil for table that doesn't exist")
	}

	// Test for a load
	starttime := time.Now().String()
	endtime := time.Now().String()
	vals := map[string]string{
		"version":   "3",
		"status":    "1",
		"starttime": starttime,
		"endtime":   endtime,
	}

	_, err = mr.Client.HSet(Ctx, table.formatLastLoadKey(), vals).Result()
	if err != nil {
		t.Fatalf("error setting test values for table1_lastload")
	}

	load, err = mr.GetLastLoad(table)
	if err != nil {
		t.Fatalf("error getting last load %s\n", err)
	}
	if load.Version != 3 || load.Status != LoadSuccess || load.StartTime != starttime || load.EndTime != endtime {
		t.Fatalf("Last load returned did not match input, expected:\n%v\nGot:\n%v\n", vals, load)
	}
}

func TestUpdateLastLoad(t *testing.T) {
	mr := newMiniRedis(t)
	table := Table{Name: "table1"}

	starttime := time.Now().String()
	endtime := time.Now().String()
	load := Load{
		Version:   3,
		Status:    LoadFailed,
		StartTime: starttime,
		EndTime:   endtime,
	}

	// update load
	err := mr.updateLastLoad(table, &load)
	if err != nil {
		t.Fatalf("failed updating load: %s\n", err)
	}

	// get load back and ensure it matches
	loadBack, err := mr.GetLastLoad(table)
	if err != nil {
		t.Fatalf("failed getting last load: %s\n", err)
	}
	if !reflect.DeepEqual(load, loadBack) {
		t.Fatalf("load given and load returned not matching\n")
	}
}

func TestBulkLoadFailures(t *testing.T) {
	mr := newMiniRedis(t)

	tableName := "table1"
	f, err := os.Open("testData/test_data_small.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Ensure fails with no schema
	err = mr.BulkLoad(tableName, f, "csv")
	if err == nil {
		t.Fatalf("BulkLoad not failing for no schema existing")
	}

	// Read xpp schema
	j, err := os.Open("testData/test_schema.json")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	jsonData, err := ioutil.ReadAll(j)
	if err != nil {
		t.Fatalf("error loading json %s", err)
	}
	var schema Schema
	json.Unmarshal(jsonData, &schema)

	// Add xpp schema
	err = mr.AddSchema(&schema)
	if err != nil {
		t.Fatalf("Failed adding schema %s\n", err)
	}

	// Ensure fails for wrong format
	err = mr.BulkLoad(tableName, f, "cs")
	if err == nil {
		t.Fatalf("BulkLoad not failing bad format")
	}
}

func TestBulkLoadSmall(t *testing.T) {
	mr := newMiniRedis(t)

	tableName, err := mr.loadXPPTestData()
	if err != nil {
		t.Fatalf("Failed loading test data %s\n", err)
	}

	table, err := mr.getTable(tableName)
	if err != nil {
		t.Fatalf("Failed getting table struct %s\n", err)
	}

	// Check last load was updated correctly
	load, err := mr.GetLastLoad(table)
	if load.Status != LoadSuccess {
		t.Fatalf("Load status not updated to success")
	}
	if load.Version != 0 {
		t.Fatalf("first load not set to 0")
	}

	// Check a few random values
	// record 18
	expected := map[string]string{
		"col2_string": "company18",
		"col1_int":    "112539291",
		"col3_string": "EMEA",
		"col4_int":    "396000.0",
	}
	record, err := mr.Client.HGetAll(Ctx, table.formatRecordKey(18)).Result()
	if err != nil {
		t.Fatalf("Failed getting record %s\n", err)
	}
	if !reflect.DeepEqual(expected, record) {
		t.Fatalf("Expected record and returned record not matching")
	}

	// record 0
	expected = map[string]string{
		"col2_string": "company1",
		"col1_int":    "846039907",
		"col3_string": "AMER",
		"col4_int":    "0.0",
	}
	record, err = mr.Client.HGetAll(Ctx, table.formatRecordKey(0)).Result()
	if err != nil {
		t.Fatalf("Failed getting record %s\n", err)
	}
	if !reflect.DeepEqual(expected, record) {
		t.Fatalf("Expected record and returned record not matching")
	}

	// record 23
	expected = map[string]string{
		"col2_string": "company23",
		"col1_int":    "114175679",
		"col3_string": "AMER",
		"col4_int":    "13100.0",
	}
	record, err = mr.Client.HGetAll(Ctx, table.formatRecordKey(23)).Result()
	if err != nil {
		t.Fatalf("Failed getting record %s\n", err)
	}
	if !reflect.DeepEqual(expected, record) {
		t.Fatalf("Expected record and returned record not matching")
	}

	// Check a few filterable columns
	// Check length of col3_string = AMER
	n, err := mr.Client.SCard(Ctx, table.formatFilterKey("col3_string", "AMER")).Result()
	if err != nil {
		t.Fatalf("Failed getting filter %s\n", err)
	}
	if n != 11 {
		t.Fatalf("Expected filter length not matching")
	}

	// Check random member is part of col3_string = EMEA
	isMember, err := mr.Client.SIsMember(Ctx,
		table.formatFilterKey("col3_string", "EMEA"),
		table.formatRecordKey(19),
	).Result()
	if err != nil {
		t.Fatalf("Failed getting filter %s\n", err)
	}
	if !isMember {
		t.Fatalf("Expected record not included in filter")
	}

	// Run 1 more time to check versioning is working
	f, _ := os.Open("testData/test_data_small.csv")
	err = mr.BulkLoad(tableName, f, "csv")
	if err != nil {
		t.Fatalf("Error running bulk load for a second time %s\n", err)
	}

	// Check last load was updated correctly
	load, err = mr.GetLastLoad(table)
	if load.Status != LoadSuccess {
		t.Fatalf("Second load status not updated to success")
	}
	if load.Version != 1 {
		t.Fatalf("Second load not set to 1")
	}
}
