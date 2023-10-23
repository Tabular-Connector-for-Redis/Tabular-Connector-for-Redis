// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type LoadStatus int

const (
	LoadFailed LoadStatus = iota
	LoadSuccess
	LoadRunning
)

type Load struct {
	Version   int
	Status    LoadStatus
	StartTime string
	EndTime   string
}

// Gets the last load for table
func (db *Database) GetLastLoad(table Table) (Load, error) {
	record, err := db.Client.HGetAll(Ctx, table.formatLastLoadKey()).Result()
	if err != nil {
		return Load{}, err
	}
	if len(record) == 0 {
		return Load{}, ErrNil
	}

	version, err := strconv.Atoi(record["version"])
	if err != nil {
		return Load{}, err
	}
	status, err := strconv.Atoi(record["status"])
	if err != nil {
		return Load{}, err
	}

	return Load{
		Version:   version,
		Status:    LoadStatus(status),
		StartTime: record["starttime"],
		EndTime:   record["endtime"],
	}, nil
}

// Updates the last load for table
func (db *Database) updateLastLoad(table Table, load *Load) error {
	vals := map[string]string{
		"version":   strconv.Itoa(load.Version),
		"status":    strconv.Itoa(int(load.Status)),
		"starttime": load.StartTime,
		"endtime":   load.EndTime,
	}
	_, err := db.Client.HSet(Ctx, table.formatLastLoadKey(), vals).Result()
	return err
}

// returns the next version number, if no loads yet, returns 0
func (db *Database) getNextTableVersion(table Table) (int, error) {
	var version int
	lastLoad, err := db.GetLastLoad(table)
	if err == ErrNil {
		version = 0
	} else if err != nil {
		return -1, err
	} else if lastLoad.Status == LoadRunning {
		return -1, errors.New("last load still running")
	} else {
		version = lastLoad.Version + 1
	}
	return version, nil
}

// Adds a recordKey to the sorted set based on the value of the column
func addSortableValToPipe(table Table, pipe *redis.Pipeliner, filterKey string, col string, val string) error {
	sortedKey := table.formatSortableKey(col)

	score, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return err
	}

	sortedMember := redis.Z{
		Score:  score,
		Member: filterKey,
	}
	(*pipe).ZAdd(Ctx, sortedKey, sortedMember)
	return nil
}

// Parses record into redis hash according to the schema, also creates filter key for columns that are filterable
func recordToPipe(table Table, pipe *redis.Pipeliner, record []string, seq int, headerMap map[int]string, schemaMap map[string]int) {
	// Format Record Key
	recordKey := table.formatRecordKey(seq)

	// Iterate through values in csv row
	var recordVals []string
	for i, val := range record {
		// Get column name based on index
		col := headerMap[i]
		recordVals = append(recordVals, col, val)

		if table.Schema.Columns[schemaMap[col]].Filterable {
			filterKey := table.formatFilterKey(col, val)
			(*pipe).SAdd(Ctx, filterKey, recordKey)

			if table.Schema.Columns[schemaMap[col]].Sortable {
				addSortableValToPipe(table, pipe, filterKey, col, val)
			}
		}
	}
	// Adding Values to record key
	(*pipe).HSet(Ctx, recordKey, recordVals)

	// Adding record key to set of all records
	sortedMember := redis.Z{
		Score:  float64(seq),
		Member: recordKey,
	}
	(*pipe).ZAdd(Ctx, table.formatAllRecordKeys(), sortedMember)
}

// parses csv data into a redis Pipeliner that loads all data and adds filter keys
func csvToPipe(f io.Reader, table Table, pipe *redis.Pipeliner) error {
	r := csv.NewReader(f)
	seq := 0

	headerMap, schemaMap, err := parseCSVHeader(r, table.Schema)
	if err != nil {
		return err
	}

	// https://levelup.gitconnected.com/easy-reading-and-writing-of-csv-files-in-go-7e5b15a73c79
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		recordToPipe(table, pipe, record, seq, headerMap, schemaMap)
		seq++
	}
	return nil
}

// Parses the first line of the csv.Reader
// Returns headerMap which maps column index to column name
// And schemaMap which maps column name to index in schema.Columns
func parseCSVHeader(r *csv.Reader, schema Schema) (map[int]string, map[string]int, error) {
	headerMap := make(map[int]string)
	schemaMap := make(map[string]int)

	record, err := r.Read()
	if err == io.EOF {
		return headerMap, schemaMap, errors.New("empty csv")
	} else if err != nil {
		return headerMap, schemaMap, err
	}

	for i, columnName := range record {
		headerMap[i] = columnName
		// Find columnName in schema
		columnFound := false
		for j, col := range schema.Columns {
			if col.Name == columnName {
				schemaMap[columnName] = j
				columnFound = true
			}
		}
		if !columnFound {
			return headerMap, schemaMap, errors.New(fmt.Sprintf("column %s not in schema", columnName))
		}
	}

	return headerMap, schemaMap, nil
}

// Loads in data from f for table. If a load is already running for table, it fails.
// format signifies how data is stored in f, options are ("csv")
func (db *Database) BulkLoad(tableName string, f io.Reader, format string) error {
	starttime := time.Now().String()

	table, err := db.getTable(tableName)
	if err != nil {
		return err
	}

	table.Version, err = db.getNextTableVersion(table)
	if err != nil {
		return err
	}

	// Update last load to load running
	curLoad := Load{
		Version:   table.Version,
		StartTime: starttime,
		EndTime:   "",
		Status:    LoadRunning,
	}
	err = db.updateLastLoad(table, &curLoad)
	if err != nil {
		return err
	}

	// Make sure we updateLastLoad before returning from this function
	// Unless successful, we will mark as LoadFailed
	curLoad.Status = LoadFailed
	defer db.updateLastLoad(table, &curLoad)

	pipe := db.Client.TxPipeline()

	if format == "csv" {
		err = csvToPipe(f, table, &pipe)
		if err != nil {
			return err
		}
	} else {
		return errors.New("invalid file format")
	}

	// create new index
	if flag.Lookup("test.v") == nil ||
		strings.HasPrefix(flag.Lookup("test.run").Value.String(), "TestIndex") {
		// create new index
		err = db.createIndexToPipe(table, &pipe)
		if err != nil {
			return err
		}
	}

	_, err = pipe.Exec(Ctx)
	if err != nil {
		return err
	}

	curLoad.EndTime = time.Now().String()
	curLoad.Status = LoadSuccess

	return nil
}

func (db *Database) CreateRecord(tableName string, f io.Reader) (int64, error) {
	reqBody := make(map[string]any)
	jsonData, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(jsonData, &reqBody)
	if err != nil {
		return 0, err
	}
	head := reqBody["records"].([]interface{})[0]
	header := make([]string, 0)
	for k, _ := range head.(map[string]interface{}) {
		header = append(header, k)
	}
	csvData := [][]string{
		header,
	}
	records := reqBody["records"].([]interface{})
	for _, record := range records {
		assertedRec := record.(map[string]interface{})
		rec := make([]string, 0)
		for _, h := range header {
			rec = append(rec, assertedRec[h].(string))
		}
		csvData = append(csvData, rec)
	}
	buffer := new(bytes.Buffer)
	csvWriter := csv.NewWriter(buffer)
	err = csvWriter.WriteAll(csvData)
	if err != nil {
		return 0, err
	}
	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	r := csv.NewReader(buffer)
	headerMap, schemaMap, err := parseCSVHeader(r, table.Schema)
	if err != nil {
		return 0, err
	}
	pipe := db.Client.TxPipeline()
	res, err := db.Client.ZRevRangeWithScores(Ctx,
		table.formatAllRecordKeys(), 0, 0).Result()
	if err != nil {
		return 0, err
	}
	seq := 0
	if len(res) != 0 {
		seq = int(res[0].Score) + 1
	}
	recCount := int64(0)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}
		recordToPipe(table, &pipe, record, seq, headerMap, schemaMap)
		seq++
		recCount++
	}
	_, err = pipe.Exec(Ctx)
	if err != nil {
		return 0, err
	}
	return recCount, nil
}
