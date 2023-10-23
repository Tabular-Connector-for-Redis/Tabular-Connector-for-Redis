// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

/*
TODO:

1. Fields that are included in the index for a schema should only be the columns that we deem "SEARCHABLE"
There will be some fields that don't want to search on for the general search case

2. Searching by a specific column. Although in that case maybe we just use the FILTER sets

3. IMPORTANT: Figure out how to enable testing for RediSearch!!!!!!
*/

// Returns the redis data_type for the index's schema
func (col *Column) columnIndexFieldType() string {
	if col.DataType == "int" || col.DataType == "float" {
		return "NUMERIC"
	}
	return "TEXT"
}

func (db *Database) createIndexToPipe(table Table, pipe *redis.Pipeliner) error {
	args := []any{
		"FT.CREATE",
		table.formatTableIndex(),
		"ON",
		"HASH",
		"PREFIX",
		"1",
		table.formatTableIndexPrefix(),
		"SCHEMA",
	}

	for _, col := range table.Schema.Columns {
		if col.Searchable {
			args = append(args, col.Name, col.columnIndexFieldType())

			if col.Sortable {
				args = append(args, "SORTABLE")
			}
		}
	}

	_, err := (*pipe).Do(Ctx, args...).Result()
	return err
}

// performs a search on the index and stores record keys in returned string
func (db *Database) searchIndexStore(table Table, searchTerm string) (string, error) {
	t := time.Now().String()
	args := []any{
		"FT.SEARCH",
		table.formatTableIndex(),
		fmt.Sprintf("%q", searchTerm), // %q adds "" around variable
		"NOCONTENT",
	}

	// do the search
	res, _ := db.Client.Do(Ctx, args...).Result()

	keys := res.([]any)
	searchStoreKey := table.formatSearchIndexStoreKey(searchTerm, t)

	// Add record keys to a set
	_, err := db.Client.SAdd(Ctx, searchStoreKey, keys[1:]).Result()
	return searchStoreKey, err
}

func (db *Database) AggregateData(tableName string, aggReq AggRequest) ([]map[string]string, error) {
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}
	args := []any{
		`FT.AGGREGATE`,
		table.formatTableIndex(),
		"*",
		"groupby"}

	args = append(args, len(aggReq.Groupby))

	for _, val := range aggReq.Groupby {
		args = append(args, fmt.Sprintf("@%s", val))
	}

	args = append(args, "reduce", aggReq.Operation)
	if strings.ToLower(aggReq.Operation) == "count" {
		args = append(args, "0", "as", fmt.Sprintf("%s_result", aggReq.Operation))
	} else {
		args = append(args, "1",
			fmt.Sprintf("@%s", aggReq.Column), "as",
			fmt.Sprintf("%s_result", aggReq.Operation))
	}

	res, err := db.Client.Do(context.Background(), args...).Result()
	if err != nil {
		return nil, err
	}

	results := res.([]interface{})
	results = results[1:]
	resSlice := make([]map[string]string, 0)

	for _, item := range results {
		itemOne := item.([]interface{})
		resMap := make(map[string]string)
		for index, _ := range itemOne {
			if index%2 == 0 && index < len(itemOne)-1 {
				resMap[itemOne[index].(string)] = itemOne[index+1].(string)
			}
		}
		resSlice = append(resSlice, resMap)
	}
	return resSlice, nil
}

func (db *Database) DeleteRecord(tableName string, reqBody RecGetDelRequest) (int64, error) {
	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	args := []any{
		`FT.SEARCH`,
		table.formatTableIndex(),
	}

	var sb strings.Builder
	for _, condition := range reqBody.Conditions {
		sb.WriteString(fmt.Sprintf("@%s:%s", condition.Column, condition.Value))
	}
	args = append(args, sb.String())
	res, err := db.Client.Do(context.Background(), args...).Result()
	if err != nil {
		return 0, err
	}
	results := res.([]interface{})
	if results[0].(int64) == 0 {
		return 0, errors.New("no records exists, correct the delete conditions")
	}
	results = results[1:]
	delRecCount := int64(0)
	for in, _ := range results {
		if in < len(results)-1 && in%2 == 0 {
			hk := results[in].(string)
			res := results[in+1]
			resOne := res.([]interface{})
			resMap := make(map[string]string)
			for index, _ := range resOne {
				if index%2 == 0 && index < len(resOne)-1 {
					resMap[resOne[index].(string)] = resOne[index+1].(string)
				}
			}
			delHashMembers := make([]string, 0)
			for k, _ := range resMap {
				delHashMembers = append(delHashMembers, k)
			}

			err = deleteRecords(db, table, delHashMembers, resMap, hk)
			if err != nil {
				return 0, err
			}
			delRecCount++
		}
	}
	return delRecCount, nil
}

func deleteRecords(db *Database, table Table, delKeys []string, resMap map[string]string, hk string) error {
	var err error
	for index := 0; index < len(delKeys); index++ {
		if table.Schema.Columns[index].Filterable {
			key := table.Schema.Columns[index].Name
			filterKey := table.formatFilterKey(key, resMap[key])
			_, err = db.Client.SRem(Ctx, filterKey, hk).Result()
			if err != nil {
				return err
			}
			if table.Schema.Columns[index].Sortable {
				sortKey := table.formatSortableKey(key)
				_, err = db.Client.ZRem(Ctx, sortKey, filterKey).Result()
				if err != nil {
					return err
				}
			}
		}
	}
	allSortKey := table.formatAllRecordKeys()
	_, err = db.Client.ZRem(Ctx, allSortKey, hk).Result()
	if err != nil {
		return err
	}
	_, err = db.Client.HDel(context.Background(), hk, delKeys...).Result()
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) UpdateRecord(tableName string, reqBody RecUpdateRequest) (int64, error) {
	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	args := []any{
		`FT.SEARCH`,
		table.formatTableIndex(),
	}

	var sb strings.Builder
	for _, condition := range reqBody.Conditions {
		sb.WriteString(fmt.Sprintf("@%s:%s", condition.Column, condition.Value))
	}
	args = append(args, sb.String())
	res, err := db.Client.Do(context.Background(), args...).Result()
	if err != nil {
		return 0, err
	}
	results := res.([]interface{})
	numRec := results[0].(int64)
	if numRec == 0 {
		return 0, errors.New("no records found, check your search criteria")
	}

	results = results[1:]
	updateRecCount := int64(0)
	for in, _ := range results {
		if in < len(results)-1 && in%2 == 0 {
			recordKey := results[in].(string)
			res := results[in+1]
			resOne := res.([]interface{})
			oldData := make(map[string]string)
			for index, _ := range resOne {
				if index%2 == 0 && index < len(resOne)-1 {
					oldData[resOne[index].(string)] = resOne[index+1].(string)
				}
			}
			changeData := make(map[string]string)
			changes := reqBody.Changes
			updatedData := make(map[string]string)
			for k, v := range oldData {
				updatedData[k] = v
			}
			for _, change := range changes {
				changeData[change.Column] = change.Value
			}

			for k, v := range changeData {
				_, ok := updatedData[k]
				if ok {
					updatedData[k] = v
				}
			}

			delKeys := make([]string, 0)
			for k, _ := range oldData {
				delKeys = append(delKeys, k)
			}

			// Clean Delete
			err = deleteRecords(db, table, delKeys, oldData, recordKey)
			if err != nil {
				return 0, err
			}
			header := make([]string, 0)
			row := make([]string, 0)
			for key, val := range updatedData {
				header = append(header, key)
				row = append(row, val)
			}
			csvData := [][]string{
				header, row,
			}

			buffer := new(bytes.Buffer)
			csvWriter := csv.NewWriter(buffer)
			err = csvWriter.WriteAll(csvData)
			if err != nil {
				return 0, err
			}

			r := csv.NewReader(buffer)
			headerMap, schemaMap, err := parseCSVHeader(r, table.Schema)
			if err != nil {
				return 0, err
			}
			pipe := db.Client.TxPipeline()
			parts := strings.Split(recordKey, ":")
			strSeq := parts[len(parts)-1]
			seq, err := strconv.Atoi(strSeq)
			if err != nil {
				return 0, err
			}
			record, err := r.Read()
			if err != nil {
				return 0, err
			}
			recordToPipe(table, &pipe, record, seq, headerMap, schemaMap)
			_, err = pipe.Exec(Ctx)
			if err != nil {
				return 0, err
			}
			updateRecCount++
		}
	}
	return updateRecCount, nil
}

func (db *Database) GetRecord(tableName string, reqBody RecGetDelRequest) ([]map[string]string, error) {
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}
	args := []any{
		`FT.SEARCH`,
		table.formatTableIndex(),
	}

	var sb strings.Builder
	for _, condition := range reqBody.Conditions {
		sb.WriteString(fmt.Sprintf("@%s:%s", condition.Column, condition.Value))
	}
	args = append(args, sb.String())
	res, err := db.Client.Do(context.Background(), args...).Result()
	if err != nil {
		return nil, err
	}

	results := res.([]interface{})
	numRecs := results[0].(int64)
	if numRecs == 0 {
		return nil, errors.New("no records found, verify the search conditions")
	}
	results = results[1:]
	resSlice := make([]map[string]string, 0)
	for index, item := range results {
		if index%2 == 1 {
			itemOne := item.([]interface{})
			resMap := make(map[string]string)
			for index, _ := range itemOne {
				if index%2 == 0 && index < len(itemOne)-1 {
					resMap[itemOne[index].(string)] = itemOne[index+1].(string)
				}
			}
			resSlice = append(resSlice, resMap)
		}
	}
	return resSlice, nil
}
