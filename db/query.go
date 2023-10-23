// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"errors"
	"fmt"
)

type Query struct {
	Filters    []Filter          `json:"filters" binding:"dive"`
	SearchTerm string            `json:"searchTerm"`
	Limit      int               `json:"limit"`
	Offset     int               `json:"offset"`
	Updates    map[string]string `json:"updates"`
}

func (schema *Schema) validateQuery(query Query) error {
	if query.Limit < -1 {
		return errors.New("invalid limit")
	}
	if query.Offset < 0 {
		return errors.New("invalid offset")
	}
	return schema.validateFilters(query.Filters)
}

// Validates the update map
// For now this just checks to make sure the column is in the schema
func (db *Database) validateUpdateValues(schema *Schema, values *map[string]string) error {
	for k := range *values {
		found := false

		for _, col := range schema.Columns {
			if k == col.Name {
				found = true
				break
			}
		}

		if !found {
			return errors.New(fmt.Sprintf("column %s not found", k))
		}
	}

	return nil
}
