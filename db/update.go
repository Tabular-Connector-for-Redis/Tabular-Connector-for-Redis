// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

// Get record and remove old filtered value from filter set
// Add new value to filter set
// If sortable, we need to insert new value into the sorted set
func (db *Database) updateFilterableRecordToPipe(table Table, pipe *redis.Pipeliner, col string, val string, key string) error {
	// get old value
	record, err := db.getRecord(key, &table.Schema)
	if err != nil {
		return err
	}
	oldVal := (*record)[col]

	// remove key from old filter set
	(*pipe).SRem(Ctx, table.formatFilterKey(col, oldVal), key)

	// add key to new filter set
	(*pipe).SAdd(Ctx, table.formatFilterKey(col, val), key)

	// If sortable we need to also add that to the sorted set
	sortable, err := table.Schema.isSortable(col)
	if err != nil {
		return err
	}
	if sortable {
		err = addSortableValToPipe(table, pipe, table.formatFilterKey(col, val), col, val)
		if err != nil {
			return err
		}
	}
	return nil
}

// Updates the given records based on values
func (db *Database) updateRecords(table Table, keys []string, values map[string]string) error {
	pipe := db.Client.TxPipeline()

	for col, val := range values {
		filterable, err := table.Schema.isFilterable(col)
		if err != nil {
			return err
		}

		for _, key := range keys {
			if filterable {
				db.updateFilterableRecordToPipe(table, &pipe, col, val, key)
			}

			// update record with new value
			pipe.HSet(Ctx, key, col, val)
		}
	}
	_, err := pipe.Exec(Ctx)
	return err
}

// Updates data based on rb.Values and rb.Filters
func (db *Database) UpdateData(tableName string, query Query) error {
	if len(query.Updates) == 0 {
		return errors.New("no values provided to update")
	}

	table, err := db.getTable(tableName)
	if err != nil {
		return err
	}

	// Ensure filters are valid
	err = table.Schema.validateQuery(query)
	if err != nil {
		return err
	}

	// Get all matching recordkeys
	recordKeys, _, err := db.getRecordKeys(table, query)

	// Update records
	db.updateRecords(table, *recordKeys, query.Updates)

	return nil
}
