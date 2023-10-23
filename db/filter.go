// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type FilterOp int

const (
	EqualTo FilterOp = iota
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
)

type Filter struct {
	Col string   `json:"col"`
	Op  FilterOp `json:"op"`
	Val []string `json:"val"`
}

func strToFilterOp(s string) (FilterOp, error) {
	s = strings.ToLower(s)
	switch s {
	case "":
		return EqualTo, nil
	case "eq":
		return EqualTo, nil
	case "gt":
		return GreaterThan, nil
	case "lt":
		return LessThan, nil
	case "gte":
		return GreaterThanOrEqual, nil
	case "lte":
		return LessThanOrEqual, nil
	default:
		return EqualTo, errors.New("op is not a correct keyword")
	}
}

// Converts op keywords to FilterOp type for JSON Unmarshalling
func (op *FilterOp) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	*op, err = strToFilterOp(s)
	return nil
}

// Converts URL Query to Filters, to be deprecated
func URLQueryToFilters(query map[string][]string) []Filter {
	filters := make([]Filter, 0, len(query))

	for col, vals := range query {
		filters = append(filters, Filter{
			Col: col,
			Op:  EqualTo, // URL Query is alwasy equal to
			Val: vals,
		})
	}
	return filters
}

// validates filters for errors
func (schema *Schema) validateFilters(filters []Filter) error {
	for _, f := range filters {
		// Find column name in schema
		found := false
		for _, col := range schema.Columns {
			if f.Col == col.Name {
				found = true

				// If op is gt or lt, column must be sortable
				if f.Op != EqualTo {
					if !col.Sortable {
						return errors.New(fmt.Sprintf("can't perform gt or lt on non-sortable column %s", f.Col))
					}
					if len(f.Val) != 1 {
						return errors.New("gt and lt ops must have only 1 val")
					}
				}

				break
			}
		}
		if !found {
			return errors.New(fmt.Sprintf("filter col %s not found in schema", f.Col))
		}
	}

	return nil
}
