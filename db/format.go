// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import "fmt"

var (
	// Redis key for all schema keys
	allSchemasKey = fmt.Sprintf("%s:schemas", Prefix)
)

func (table *Table) formatKeyPrefix() string {
	return fmt.Sprintf("%s:%s:%d", Prefix, table.Name, table.Version)
}

func (table *Table) formatTableIndex() string {
	return table.formatKeyPrefix()
}

func (table *Table) formatTableIndexPrefix() string {
	return table.formatTableIndex() + ":"
}

// Returns key for a record
func (table *Table) formatRecordKey(seq int) string {
	return fmt.Sprintf("%s:%d", table.formatKeyPrefix(), seq)
}

// Returns key to the sorted set for a table, column and version
func (table *Table) formatSortableKey(col string) string {
	return fmt.Sprintf("%s:%s", table.formatKeyPrefix(), col)
}

// returns filter key
func (table *Table) formatFilterKey(col string, val string) string {
	return fmt.Sprintf("%s:%s:%s", table.formatKeyPrefix(), col, val)
}

// Returns redis schema key for a table
func (schema *Schema) formatSchemaKey() string {
	return fmt.Sprintf("%s:%s:schema", Prefix, schema.Name)
}

// Returns redis schema key for a table
func formatSchemaKey(name string) string {
	return fmt.Sprintf("%s:%s:schema", Prefix, name)
}

// Returns key for a table's last load
func (table *Table) formatLastLoadKey() string {
	return fmt.Sprintf("%s:%s:lastload", Prefix, table.Name)
}

func (table *Table) formatAllRecordKeys() string {
	return fmt.Sprintf("%s:all", table.formatKeyPrefix())
}

// Return key for a Union Store from filters
// {Prefix}:{table}:{version}:unionstore:{col}:{_vals[0]__vals[1]...__vals[n]_}:{t}
func (table *Table) formatUnionStoreKey(col string, vals []string, t string) string {
	key := fmt.Sprintf("%s:unionstore:%s", table.formatKeyPrefix(), col)
	for _, v := range vals {
		key += fmt.Sprintf("_%s_", v)
	}
	key += ":" + t
	return key
}

// Return key for intersection
// {Prefix}:{table}:{version}:interstore:[_{unionKeys[0]}__{unionKeys[1]}...__{unionKeys[n]}_}:{t}
func (table *Table) formatInterStoreKey(unionKeys []string, t string) string {
	key := fmt.Sprintf("%s:interstore:", table.formatKeyPrefix())
	for _, k := range unionKeys {
		key += fmt.Sprintf("_%s_", k)
	}
	key += ":" + t
	return key
}

// returns key to the set of record ids
func (table *Table) formatSearchIndexStoreKey(searchTerm, t string) string {
	return fmt.Sprintf("%s:searchstore:%s:%s", table.formatKeyPrefix(), searchTerm, t)
}
