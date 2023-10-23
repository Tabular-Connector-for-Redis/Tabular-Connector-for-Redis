// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Column struct {
	Name       string `json:"name" binding:"required"`
	DataType   string `json:"datatype" binding:"required"`
	Filterable bool   `json:"filterable"`
	Sortable   bool   `json:"sortable"`

	// If true, column is added to RediSearch Index's schema
	// Searches will include this column
	Searchable bool `json:"searchable"`
}

type Schema struct {
	Name    string   `json:"name" binding:"required"`
	Columns []Column `json:"columns" binding:"required,dive"`
}

func sortableDataType(dt string) bool {
	if dt == "int" || dt == "float" {
		return true
	}
	return false
}

// validates the schema
// For now this only makes sure all sortable columns are also filterable
func validateSchema(schema *Schema) error {
	for _, c := range schema.Columns {
		if c.Sortable {
			// Must be filterable
			if !c.Filterable {
				return errors.New(fmt.Sprintf("invalid schema %s is sortable but not filterable", c.Name))
			}
			if !sortableDataType(c.DataType) {
				return errors.New(fmt.Sprintf("invalid schema %s datatype is not sortable", c.Name))
			}
		}
	}
	return nil
}

// AddSchema attempts to add schema to the Database and also
// adds the schema key to the set of schema keys
// if the schema.name already exists, an error is returned
func (db *Database) AddSchema(schema *Schema) error {
	err := validateSchema(schema)
	if err != nil {
		return err
	}

	key := schema.formatSchemaKey()

	// Check if the schema already exists
	s, _ := db.Client.Get(Ctx, key).Result()
	if s != "" {
		return ErrImmutableKey
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	pipe := db.Client.TxPipeline()
	// add schema key to schemas set
	pipe.SAdd(Ctx, allSchemasKey, key)
	// Add schema json
	pipe.Set(Ctx, key, schemaJSON, 0)
	_, err = pipe.Exec(Ctx)
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) getSchemaByKey(key string) (*Schema, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	schemaJSON, err := db.Client.Get(Ctx, key).Result()
	if err != nil {
		if schemaJSON == "" {
			return nil, ErrNil
		}
		return nil, err
	}

	var schema Schema
	err = json.Unmarshal([]byte(schemaJSON), &schema)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}

// GetSchema returns the schema based on table name
func (db *Database) GetSchema(name string) (*Schema, error) {
	if name == "" {
		return nil, ErrEmptyKey
	}
	return db.getSchemaByKey(formatSchemaKey(name))
}

// GetAllSchemas returns all of the schemas with keys in the schema key set
func (db *Database) GetAllSchemas() (*[]Schema, error) {
	keys, err := db.Client.SMembers(Ctx, allSchemasKey).Result()
	if err != nil {
		return nil, err
	}

	var schemas []Schema

	for _, key := range keys {
		s, err := db.getSchemaByKey(key)
		// TODO: in the future, if there is an error should we just skip this schema?
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, *s)
	}

	return &schemas, nil
}

func (schema *Schema) isFilterable(col string) (bool, error) {
	for _, c := range schema.Columns {
		if c.Name == col {
			return c.Filterable, nil
		}
	}
	return false, errors.New(fmt.Sprintf("column %s not found in schema", col))
}

func (schema *Schema) isSortable(col string) (bool, error) {
	for _, c := range schema.Columns {
		if c.Name == col {
			return c.Sortable, nil
		}
	}
	return false, errors.New(fmt.Sprintf("column %s not found in schema", col))
}
