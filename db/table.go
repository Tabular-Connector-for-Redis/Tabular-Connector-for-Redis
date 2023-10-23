// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

type Table struct {
	Schema  Schema
	Version int
	Name    string
}

func (db *Database) getTable(name string) (Table, error) {
	table := Table{Name: name}

	// Get table schema
	schema, err := db.GetSchema(name)
	if err != nil {
		return table, err
	}
	table.Schema = *schema

	// get last load to get table version
	load, err := db.GetLastLoad(table)
	if err != nil && err != ErrNil {
		return table, err
	}
	version := load.Version

	return Table{
		Schema:  *schema,
		Version: version,
		Name:    name,
	}, nil
}
