// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type ResultSet struct {
	Count  int `json:"count"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
	Total  int `json:"total"`
}

type AggRequest struct {
	Operation string   `json:"operation" binding:"required"`
	Column    string   `json:"column" binding:"required"`
	Groupby   []string `json:"group_by" binding:"required"`
}

type Condition struct {
	Column string `json:"column" binding:"required"`
	Value  string `json:"value" binding:"required"`
}

type Change struct {
	Column string `json:"column" binding:"required"`
	Value  string `json:"value" binding:"required"`
}

type RecUpdateRequest struct {
	Conditions []Condition `json:"conditions" binding:"required,dive"`
	Changes    []Change    `json:"changes" binding:"required,dive"`
}

type RecGetDelRequest struct {
	Conditions []Condition `json:"conditions" binding:"required,dive"`
}

type Metadata struct {
	ResultSet ResultSet `json:"result_set"`
}

type GetDataResponse struct {
	Records  []map[string]string `json:"records"`
	Metadata Metadata            `json:"metadata"`
}

type WorkerJobs struct {
	Key   string
	Index int
}

type WorkerResult struct {
	Record *map[string]string
	Index  int
	err    error
}

// Returns the paged records from the given key to a sorted set
// If limit = -1 then no limit
func (db *Database) getPagedRecordKeys(key string, limit int, offset int) (*[]string, ResultSet, error) {
	var keys []string
	var err error
	var start, stop int64
	var resultSet ResultSet

	start = int64(offset)
	if limit <= 0 {
		stop = -1
	} else {
		stop = int64(offset) + int64(limit) - 1
	}
	keys, err = db.Client.ZRange(Ctx, key, start, stop).Result()
	if err != nil {
		return nil, ResultSet{}, err
	}

	total, err := db.Client.ZCard(Ctx, key).Result()
	if err != nil {
		return nil, ResultSet{}, err
	}

	resultSet.Count = len(keys)
	resultSet.Offset = offset
	resultSet.Limit = limit
	resultSet.Total = int(total)
	return &keys, resultSet, nil
}

// // Returns all record keys for a table
// func (db *Database) getAllRecordKeys(table string, version int, limit int, offset int, searchStoreKey string) (string, error) {
// 	// keys, err := db.Client.SMembers(Ctx, formatAllRecordKeys(table, version)).Result()
// 	return db.getPagedRecordKeys(formatAllRecordKeys(table, version), limit, offset)
// }

// Gets all of the filter keys for a given filter by performing
// ZRANGEBYSCORE command to get all of the filter keys in the ordered set
func (db *Database) getOrderedFilterKeys(table Table, f Filter) ([]string, error) {
	key := table.formatSortableKey(f.Col)
	var r redis.ZRangeBy

	switch f.Op {
	case GreaterThan:
		r.Min = "(" + f.Val[0]
		r.Max = "+inf"
	case LessThan:
		r.Min = "-inf"
		r.Max = "(" + f.Val[0]
	case GreaterThanOrEqual:
		r.Min = f.Val[0]
		r.Max = "+inf"
	case LessThanOrEqual:
		r.Min = "-inf"
		r.Max = f.Val[0]
	}

	return db.Client.ZRangeByScore(Ctx, key, &r).Result()
}

// For a given filter, gets all of the filterKeys, runs a UNION and appends destintion to unionkeys
func (db *Database) addFilterUnionKey(table Table, unionKeys *[]string, t string, f Filter) error {
	// format UNIONSTORE destination
	dst := table.formatUnionStoreKey(f.Col, f.Val, t)

	// Get all filterkeys
	filterKeys := make([]string, 0, len(f.Val))

	if f.Op == EqualTo {
		for _, v := range f.Val {
			filterKeys = append(filterKeys, table.formatFilterKey(f.Col, v))
		}
		// EqualTo
	} else {
		fs, err := db.getOrderedFilterKeys(table, f)
		if err != nil {
			return err
		}
		filterKeys = append(filterKeys, fs...)
	}

	// Execute UnionStore
	n, err := db.Client.SUnionStore(Ctx, dst, filterKeys...).Result()
	*unionKeys = append(*unionKeys, dst)
	if n == 0 {
		return ErrNil
	}
	return err
}

// Returns all record keys based on filters provided
// Runs a SUNIONSTORE on all values for each filtered columns
// Runs a SINTER of all stored union values
func (db *Database) getFilteredRecordKeys(table Table, filters []Filter) (string, error) {
	t := time.Now().String()

	// var unionKeys []string
	unionKeys := make([]string, 0, len(filters))

	// Loop through all filtered columns and use UNIONSTORE to store all keys
	for _, f := range filters {
		err := db.addFilterUnionKey(table, &unionKeys, t, f)
		if err != nil {
			return "", err
		}
	}

	// get intersection of all UNIONSTORES
	interKey := table.formatInterStoreKey(unionKeys, t)
	_, err := db.Client.SInterStore(Ctx, interKey, unionKeys...).Result()

	// Must do a zinterstore instead of zinter for testing purposes, miniredis has not implemented zinter
	finalKey := interKey + "_final"
	_, err = db.Client.ZInterStore(Ctx, finalKey,
		&redis.ZStore{
			Keys: []string{table.formatAllRecordKeys(), interKey},
		}).Result()
	if err != nil {
		return "", err
	}

	// keys, err := db.Client.ZRange(Ctx, finalKey, 0, -1).Result()
	return finalKey, nil
}

// Return All Record Keys based on parameters and filters
func (db *Database) getRecordKeys(table Table, query Query) (*[]string, ResultSet, error) {
	// var searchStoreKey string
	var err error

	finalKey := table.formatAllRecordKeys()
	// Get record keys matching the filters
	if len(query.Filters) > 0 {
		finalKey, err = db.getFilteredRecordKeys(table, query.Filters)
		if err != nil {
			return nil, ResultSet{}, err
		}
	}

	// // If there is a searchTerm, perform search and get key to set or recordKeys
	// if query.SearchTerm != "" {
	// 	searchStoreKey, err = db.searchIndexStore(table, query.SearchTerm)
	// 	if err != nil {
	// 		return nil, ResultSet{}, err
	// 	}

	// 	// Run Zinter with the search keys and replace finalKey set
	// 	_, err = db.Client.ZInterStore(Ctx, searchStoreKey,
	// 		&redis.ZStore{
	// 			Keys: []string{finalKey, searchStoreKey},
	// 		}).Result()
	// 	if err != nil {
	// 		return nil, ResultSet{}, err
	// 	}

	// 	finalKey = searchStoreKey
	// }

	return db.getPagedRecordKeys(finalKey, query.Limit, query.Offset)
}

// Returns record based on key and checks to make sure record matches schema
func (db *Database) getRecord(key string, schema *Schema) (*map[string]string, error) {
	record, err := db.Client.HGetAll(Ctx, key).Result()
	if len(record) == 0 {
		return &record, ErrNil
	}

	// Make sure no missing, or additional columns
	if len((*schema).Columns) != len(record) {
		return nil, errors.New(fmt.Sprintf("Number of columns for key %s do not match schema", key))
	}
	for _, schemaCol := range (*schema).Columns {
		if _, ok := record[schemaCol.Name]; !ok {
			return nil, errors.New(fmt.Sprintf("%s column not found in record %s", schemaCol.Name, key))
		}
	}

	return &record, err
}

// Gets keys from the jobs channel and sends the record to results channel
func (db *Database) getRecordsWorker(id int, schema *Schema, jobs <-chan WorkerJobs, results chan<- *WorkerResult) {
	for j := range jobs {
		record, err := db.getRecord(j.Key, schema)
		results <- &WorkerResult{Record: record, Index: j.Index, err: err}
		if err != nil {
			return
		}
	}
}

// Creates a worker pool using recordKeys as jobs, and the returned record as results
// https://gobyexample.com/waitgroups
// https://gobyexample.com/worker-pools
func (db *Database) getRecordsWorkerPool(keys *[]string, schema *Schema) (*GetDataResponse, error) {
	tableData := GetDataResponse{
		Records: make([]map[string]string, len(*keys), len(*keys)),
	}

	// queue of keys to get
	jobs := make(chan WorkerJobs, len(*keys))

	// queue of records returned
	results := make(chan *WorkerResult, len(*keys))

	// Send off workers
	var wg sync.WaitGroup

	for w := 1; w <= MaxWorkers; w++ {
		wg.Add(1)

		// Avoid re-use of the same w value in each goroutine closure. See the FAQ for more details.
		w := w

		go func() {
			defer wg.Done()
			db.getRecordsWorker(w, schema, jobs, results)
		}()
	}

	for i, key := range *keys {
		jobs <- WorkerJobs{Key: key, Index: i}
	}
	close(jobs)

	// This will wait until until workers are done and then close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Continuously read from results channel and add to tableData at the correct index
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		tableData.Records[(*res).Index] = *res.Record
	}

	return &tableData, nil
}

// Returns data for a given table
// TODO include filters
func (db *Database) GetData(tableName string, query Query) (*GetDataResponse, error) {
	table, err := db.getTable(tableName)

	// Ensure filters are valid
	err = table.Schema.validateQuery(query)
	if err != nil {
		return nil, err
	}

	// get all recordKeys
	keys, resultSet, err := db.getRecordKeys(table, query)
	if err != nil {
		return nil, err
	}

	// Create worker pool to get records quicker
	tableData, err := db.getRecordsWorkerPool(keys, &table.Schema)
	if err != nil {
		return nil, err
	}

	tableData.Metadata.ResultSet = resultSet

	return tableData, nil
}
