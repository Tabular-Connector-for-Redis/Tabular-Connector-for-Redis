// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package db

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestAddSchema(t *testing.T) {
	// https: //github.com/alicebob/miniredis
	mr := newMiniRedis(t)

	testSchemaJSON1, _ := json.Marshal(testSchema1)

	// Add Schema to client
	err := mr.AddSchema(&testSchema1)
	if err != nil {
		t.Fatalf("AddSchema(&testSchema1) error: %s", err)
	}

	// Get the schema value back and check to make sure it matches the expected json
	val, err := mr.Client.Get(Ctx, formatSchemaKey(testSchema1.Name)).Result()
	if val != string(testSchemaJSON1) || err != nil {
		t.Fatalf(`AddSchema(&testSchema1) = %q, %v, want match for %#q, nil`, val, err, testSchemaJSON1)
	}

	// Ensure the key is in the schema keys set
	in, err := mr.Client.SIsMember(Ctx, allSchemasKey, formatSchemaKey(testSchema1.Name)).Result()
	if !in {
		t.Fatalf("AddSchema(&testSchema1): %s not in rdb_schemas set", formatSchemaKey(testSchema1.Name))
	}
	if err != nil {
		t.Fatalf("AddSchema(&testSchema1) error: %s", err)
	}

	// Adding schema that already exists
	err = mr.AddSchema(&testSchema1)
	if err != ErrImmutableKey {
		t.Fatalf("AddSchema(&testSchema1) error: did not return schema already exists")
	}

	/*
		Testing if table name, column name, or data type is empty is unnecessary because it is handled using
		Gin's c.ShouldBindJSON function with the binding:"required" in our struct definition

		//https://gin-gonic.com/docs/examples/binding-and-validation/
		//https://github.com/gin-gonic/gin/issues/3436

		// Adding schema with empty table name
		testSchema2 := testSchema1
		testSchema2.Name = ""
		err = mr.AddSchema(&testSchema2)
		if err == nil {
			t.Fatal("AddSchema(&testSchema2) error: successfully added schema with empty table name")
		}

		// Adding schema with empty table name
		testSchema2.Name = "table2"
		testSchema2.Columns[1].Name = ""
		err = mr.AddSchema(&testSchema2)
		if err == nil {
			t.Fatal("AddSchema(&testSchema2) error: successfully added schema with empty column name")
		}

		// Adding schema with empty table name
		testSchema2.Columns[1].Name = "col2"
		testSchema2.Columns[2].Datatype = ""
		err = mr.AddSchema(&testSchema2)
		if err == nil {
			t.Fatal("AddSchema(&testSchema2) error: successfully added schema with empty column datatype")
		}
	*/
}

func TestGetSchema(t *testing.T) {
	mr := newMiniRedis(t)

	// Test getting a schema that exists properly
	testSchemaJSON1, _ := json.Marshal(testSchema1)
	err := mr.Client.Set(Ctx, formatSchemaKey(testSchema1.Name), string(testSchemaJSON1), 0).Err()
	if err != nil {
		t.Fatalf("error setting testSchema1: %s", err)
	}

	schema, err := mr.GetSchema(testSchema1.Name)
	if !reflect.DeepEqual(*schema, testSchema1) || err != nil {
		t.Fatalf(`GetSchema(testSchema1.Name, true) = %#v, %v, want match for %#v, nil`, *schema, err, testSchema1)
	}

	// Test getting a schema that does not exist
	schema, err = mr.GetSchema("missing_table")
	if err != ErrNil {
		t.Fatalf("Get schema did not fail getting a schema that does not exist\n")
	}

	// Test getting an empty table
	schema, err = mr.GetSchema("")
	if err != ErrEmptyKey {
		t.Fatalf("Get schema did not fail getting and empty key\n")
	}
}

func TestGetAllSchemas(t *testing.T) {
	mr := newMiniRedis(t)

	// Test for no schemas
	schemas, err := mr.GetAllSchemas()
	if err != nil {
		t.Fatalf("error getting all schemas when empty: %s", err)
	}
	if len(*schemas) != 0 {
		t.Fatalf("error len(*schemas) != 0 for no schemas")
	}

	// Add some test schemas
	err = mr.AddSchema(&testSchema1)
	if err != nil {
		t.Fatalf("error adding testSchema1: %s", err)
	}
	err = mr.AddSchema(&testSchema2)
	if err != nil {
		t.Fatalf("error setting testSchema2: %s", err)
	}

	// Ensure schemas match expected
	schemas, err = mr.GetAllSchemas()
	if err != nil {
		t.Fatalf("error getting schemas: %s", err)
	}
	if len(*schemas) != 2 {
		t.Fatalf("Expected 2 schemas, got %d", len(*schemas))
	}
	if (*schemas)[0].Name == testSchema1.Name {
		if !reflect.DeepEqual((*schemas)[0], testSchema1) || !reflect.DeepEqual((*schemas)[1], testSchema2) {
			t.Fatalf("Get all (*schemas) do not match expected")
		}
	} else {
		if !reflect.DeepEqual((*schemas)[1], testSchema1) || !reflect.DeepEqual((*schemas)[0], testSchema2) {
			t.Fatalf("Get all schemas do not match expected")
		}
	}
}
