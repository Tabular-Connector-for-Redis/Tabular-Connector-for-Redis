// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	bytes2 "bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

const (
	TestRedisPort = 6389
	TestStackName = "redis-test-stack"
	TableName     = "table1"
)

var db *Database

func setUp() error {
	l, errPort := net.Listen("tcp", ":"+strconv.Itoa(TestRedisPort))
	if errPort == nil {
		_ = l.Close()

		// 1. Start the test container
		command := "docker"
		args := []string{
			"run",
			"-d",
			"--name",
			TestStackName,
			"-p",
			"6389:6379",
			"-p",
			"8005:8005",
			"redis/redis-stack:latest",
		}

		_, err := exec.Command(command, args...).Output()
		if err != nil {
			return err
		}
	}

	// 2. Create Redis Client Connection
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", TestRedisPort),
		Password: "",
		DB:       0,
	})

	// Checks to make sure client is alive
	if err := client.Ping(Ctx).Err(); err != nil {
		return err
	}

	db = &Database{
		Client: client,
	}

	// 3. Add Schema
	if errPort == nil {
		schema := Schema{
			Name: TableName,
			Columns: []Column{
				{
					Name:       "col1_int",
					DataType:   "string",
					Filterable: false,
					Sortable:   false,
					Searchable: true,
				},
				{
					Name:       "col2_string",
					DataType:   "string",
					Filterable: true,
					Sortable:   false,
					Searchable: true,
				},
				{
					Name:       "col3_string",
					DataType:   "string",
					Filterable: true,
					Sortable:   false,
					Searchable: true,
				},
				{
					Name:       "col4_int",
					DataType:   "float",
					Filterable: true,
					Sortable:   true,
					Searchable: true,
				},
			},
		}
		if err := db.AddSchema(&schema); err != nil {
			return err
		}
	}
	return nil
}

func tearDown() {
	command := "docker"
	args := []string{
		"rm",
		"-f",
		TestStackName,
	}
	_, err := exec.Command(command, args...).Output()
	if err != nil {
	}
}

func loadData() error {
	csvData := [][]string{
		{"col2_string", "col1_int", "col3_string", "col4_int"},
		{"CIBC", "300", "EMEA", "10"},
		{"VMW", "500", "AMER", "50"},
	}
	buffer := new(bytes2.Buffer)
	csvWriter := csv.NewWriter(buffer)
	err := csvWriter.WriteAll(csvData)
	if err != nil {
		return err
	}
	err = db.BulkLoad(TableName, buffer, "csv")
	if err != nil {
		return errors.New("failed to create records")
	}
	return nil
}

func cleanData() {
	delReq := []RecGetDelRequest{
		{
			Conditions: []Condition{
				{
					Column: "col3_string",
					Value:  "AMER",
				},
			},
		},
		{
			Conditions: []Condition{
				{
					Column: "col3_string",
					Value:  "EMEA",
				},
			},
		},
	}
	for _, del := range delReq {
		_, _ = db.DeleteRecord(TableName, del)
	}
}

func TestIndex_CreateIndex(t *testing.T) {
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}

	table, err := db.getTable(TableName)
	if err != nil {
		t.Errorf("Failed to get table with error %s", err.Error())
	}

	pipe := db.Client.TxPipeline()
	if err := db.createIndexToPipe(table, &pipe); err != nil {
		t.Errorf("Failed to create index with error %s", err.Error())
	}
}

func TestIndex_SearchIndexStore(t *testing.T) {
	defer cleanData()
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}
	table, err := db.getTable(TableName)
	if err != nil {
		t.Errorf("Failed to get table with error %s", err.Error())
	}
	if err := loadData(); err != nil {
		t.Errorf("Failed to load data due to error : %s", err.Error())
	}

	_, err = db.searchIndexStore(table, "AMER")
	if err != nil {
		t.Errorf("Search failed with erro %s", err.Error())
	}
}

func TestIndex_AggregateData(t *testing.T) {
	defer cleanData()
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}

	if err := loadData(); err != nil {
		t.Errorf("Failed to load data due to error : %s", err.Error())
	}
	AggReq := AggRequest{
		Operation: "sum",
		Column:    "col4_int",
		Groupby: []string{
			"col3_string",
		},
	}
	res, err := db.AggregateData(TableName, AggReq)
	if err != nil || len(res) != 2 {
		t.Errorf("Aggreation failed due to error %s", err.Error())
	}
	for _, r := range res {
		if r["col3_string"] == "EMEA" && r["sum_result"] != "10" {
			t.Errorf("Exepecte SUM is : %s but got : %s", "10", r["sum_result"])
		}
		if r["col3_string"] == "AMER" && r["sum_result"] != "50" {
			t.Errorf("Exepecte SUM is : %s but got : %s", "10", r["sum_result"])
		}
	}
	AggReq = AggRequest{
		Operation: "count",
		Column:    "col4_int",
		Groupby: []string{
			"col3_string",
		},
	}
	res, err = db.AggregateData(TableName, AggReq)
	if err != nil || len(res) != 2 {
		t.Errorf("Aggreation failed due to error %s", err.Error())
	}

	for _, r := range res {
		if r["col3_string"] == "EMEA" && r["count_result"] != "1" {
			t.Errorf("Exepecte COUNT is : %s but got : %s", "1", r["count_result"])
		}
		if r["col3_string"] == "AMER" && r["count_result"] != "1" {
			t.Errorf("Exepecte COUNT is : %s but got : %s", "1", r["count_result"])
		}
	}
}

func TestIndex_DeleteRecord(t *testing.T) {
	defer cleanData()
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}

	if err := loadData(); err != nil {
		t.Errorf("Failed to load data due to error : %s", err.Error())
	}
	delReq := RecGetDelRequest{
		Conditions: []Condition{
			{
				Column: "col3_string",
				Value:  "AMER",
			},
		},
	}
	count, err := db.DeleteRecord(TableName, delReq)
	if err != nil {
		t.Errorf("DeleteRecord failed due to error : %s", err.Error())
	}
	if count != 1 {
		t.Errorf("Expected to delete 1 record but deleted %d records", count)
	}
}

func TestIndex_UpdateRecord(t *testing.T) {
	defer cleanData()
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}

	if err := loadData(); err != nil {
		t.Errorf("Failed to load data due to error : %s", err.Error())
	}

	updateRed := RecUpdateRequest{
		Conditions: []Condition{
			{
				Column: "col2_string",
				Value:  "CIBC",
			},
		},
		Changes: []Change{
			{
				Column: "col4_int",
				Value:  "111",
			},
		},
	}
	count, err := db.UpdateRecord(TableName, updateRed)
	if err != nil {
		t.Errorf("DeleteRecord failed due to error : %s", err.Error())
	}
	if count != 1 {
		t.Errorf("UpdateRecord failed, Expected to update 1 record, but updated %d records", count)
	}

	getReq := RecGetDelRequest{
		Conditions: []Condition{
			{
				Column: "col2_string",
				Value:  "CIBC",
			},
		},
	}
	res, err := db.GetRecord(TableName, getReq)
	if err != nil {
		t.Errorf("GetRecord failed due to error : %s", err.Error())
	}
	for _, r := range res {
		if r["col2_string"] == "CIBC" && r["col4_int"] != "111" {
			t.Errorf("UpdateRecord failed to update value %s", r["col4_int"])
		}
	}
}

func TestIndex_GetRecord(t *testing.T) {
	defer tearDown()
	if err := setUp(); err != nil {
		t.Errorf("Test setup failed with err : %s", err.Error())
	}

	if err := loadData(); err != nil {
		t.Errorf("Failed to load data due to error : %s", err.Error())
	}
	getReq := RecGetDelRequest{
		Conditions: []Condition{
			{
				Column: "col3_string",
				Value:  "EMEA",
			},
		},
	}
	res, err := db.GetRecord(TableName, getReq)
	if err != nil {
		t.Errorf("GetRecord failed due to error : %s", err.Error())
	}
	if len(res) != 1 {
		t.Errorf("Expected to get 1 record but got %d records", len(res))
	}

	for _, r := range res {
		if r["col3_string"] == "EMEA" {
			if r["col2_string"] != "CIBC" || r["col1_int"] != "300" || r["col4_int"] != "10" {
				t.Errorf("GetRecord failed")
			}
		}
	}
}
