// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause

package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"rdb/db"
	"strconv"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

var (
	// https://rollbar.com/blog/golang-error-logging-guide/
	WarningLog *log.Logger
	InfoLog    *log.Logger
	ErrorLog   *log.Logger
)

// https://blog.logrocket.com/how-to-use-redis-as-a-database-with-go-redis/
func main() {
	// Initializing loggers
	WarningLog = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	InfoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	err := GetConfig()
	if err != nil {
		ErrorLog.Fatal("error parsing config: ", err)
	}

	database, err := db.NewDatabase(getRedisAddr(), viper.Get("redis.prefix").(string), viper.Get("redis.password").(string))
	if err != nil {
		ErrorLog.Fatalf("Failed to connect to redis: %s", err.Error())
	}
	InfoLog.Println("succesfully connected to redis")

	router := initRouter(database)
	router.Run(getServerAddr())
}

func initRouter(database *db.Database) *gin.Engine {
	router := gin.Default()
	router.Use(gzip.Gzip(gzip.BestSpeed))

	router.POST("/api/v1/schema", func(c *gin.Context) {
		// TODO needs to be a primary key
		// If no primary key, it can't be transactional (full load only)
		var schema db.Schema
		err := c.ShouldBindJSON(&schema)
		if err != nil {
			ErrorLog.Println("error binding json to schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err = database.AddSchema(&schema)
		if err != nil {
			ErrorLog.Println("error adding schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Printf("successfully added schema for %s\n", schema.Name)

		c.JSON(http.StatusOK, gin.H{"schema": schema})
	})
	router.GET("/api/v1/schema/:table", func(c *gin.Context) {
		table := c.Param("table")
		InfoLog.Printf("retrieving schema for %s\n", table)

		schema, err := database.GetSchema(table)
		if err != nil {
			if err == db.ErrNil {
				ErrorLog.Printf("error: no record for %s\n", table)
				c.JSON(http.StatusNotFound, gin.H{"error": "No record found for " + table})
				return
			}

			ErrorLog.Printf("error retrieving schema for %s: %s\n", table, err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Printf("successfully retrieved schema for %s\n", table)

		c.JSON(http.StatusOK, gin.H{"schema": schema})
	})
	//TODO GET /schema/ returns all schemas
	router.GET("/api/v1/schema", func(c *gin.Context) {
		InfoLog.Println("retrieving all schemas")

		schemas, err := database.GetAllSchemas()
		if err != nil {
			ErrorLog.Println("error retreiving all schema:", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		InfoLog.Println("successfully retrieved all schemas")
		c.JSON(http.StatusOK, gin.H{"schemas": schemas})
	})
	router.POST("/api/v1/schema/:table/load", func(c *gin.Context) {
		table := c.Param("table")
		InfoLog.Printf("Loading data for %s\n", table)

		err := database.BulkLoad(table, c.Request.Body, "csv")
		if err != nil {
			ErrorLog.Println("error loading data:", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// TODO Add response for success
		InfoLog.Println("successfully loaded data")
		c.JSON(http.StatusOK, gin.H{"Status": "Succesfully loaded data"})
	})
	// TODO GET /api/v1/schema/:table/data
	router.GET("/api/v1/schema/:table/data", func(c *gin.Context) {
		// Get filters from body
		// var filters db.RequestBody
		var query db.Query
		err := c.ShouldBindJSON(&query)
		if err != nil && err != io.EOF {
			ErrorLog.Println("error binding json to schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// get pagination parameters
		params := c.Request.URL.Query()
		pageLimit, pageOffset, err := getPaginationParams(params)
		if err != nil {
			ErrorLog.Println("error getting pagination parameters: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		query.Limit = pageLimit
		query.Offset = pageOffset

		// Get data for table
		table := c.Param("table")
		InfoLog.Printf("Getting data for %s\n", table)
		getDataResp, err := database.GetData(table, query)
		if err != nil {
			ErrorLog.Println("error retreiving data:", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		resp := gin.H{
			"records":  (*getDataResp).Records,
			"metadata": (*getDataResp).Metadata,
		}

		InfoLog.Println("successfully retrieved all data")
		c.JSON(http.StatusOK, resp)
	})
	router.PATCH("/api/v1/schema/:table/update", func(c *gin.Context) {
		// Get filters and values from body
		var query db.Query
		err := c.ShouldBindJSON(&query)
		if err != nil && err != io.EOF {
			ErrorLog.Println("error binding json to schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Update datai
		table := c.Param("table")
		InfoLog.Printf("Updating data for %s\n", table)

		err = database.UpdateData(table, query)
		if err != nil {
			ErrorLog.Println("error updating data:", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		InfoLog.Println("successfully updated records")
		c.JSON(http.StatusOK, gin.H{"Status": "Succesfully updated records"})
	})

	// TODO can redis perform aggregation (ex: sum, count)
	router.GET("/api/v1/schema/:table/agg", func(c *gin.Context) {
		tableName := c.Param("table")
		var aggRequest db.AggRequest
		err := c.BindJSON(&aggRequest)
		if err != nil {
			ErrorLog.Println("error binding json to schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		response, err := database.AggregateData(tableName, aggRequest)
		if err != nil {
			ErrorLog.Println("error in performing aggregation:", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		InfoLog.Println("successfully performed aggregation")
		c.JSON(http.StatusOK, gin.H{"records": response})
	})

	router.POST("/api/v1/schema/:table/record", func(c *gin.Context) {
		tableName := c.Param("table")
		recCount, err := database.CreateRecord(tableName, c.Request.Body)
		if err != nil {
			ErrorLog.Println("error in adding the record", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Println("successfully added records")
		c.JSON(http.StatusOK, gin.H{
			"created_records_count": recCount,
		})
	})

	router.DELETE("/api/v1/schema/:table/record", func(c *gin.Context) {
		var recGetDelRequest db.RecGetDelRequest
		err := c.BindJSON(&recGetDelRequest)
		if err != nil {
			ErrorLog.Println("error binding json to schema: ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		tableName := c.Param("table")
		delRecCount, err := database.DeleteRecord(tableName, recGetDelRequest)
		if err != nil {
			ErrorLog.Println("error in deleting the record", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Println("successfully deleted the record")
		c.JSON(http.StatusOK, gin.H{
			"deleted_records_count": delRecCount,
		})
	})

	router.GET("/api/v1/schema/:table/record", func(c *gin.Context) {
		tableName := c.Param("table")
		var getRecRequest db.RecGetDelRequest
		err := c.ShouldBindJSON(&getRecRequest)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		response, err := database.GetRecord(tableName, getRecRequest)
		if err != nil {
			ErrorLog.Println("error in getting the record", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Println("successfully retrieved the record")
		c.JSON(http.StatusOK, gin.H{
			"records": response,
		})
	})

	router.PATCH("/api/v1/schema/:table/record", func(c *gin.Context) {
		tableName := c.Param("table")
		var reqBody db.RecUpdateRequest
		err := c.ShouldBindJSON(&reqBody)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		updateRecCount, err := database.UpdateRecord(tableName, reqBody)
		if err != nil {
			ErrorLog.Println("error in updating the record", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		InfoLog.Println("successfully updated the record")
		c.JSON(http.StatusOK, gin.H{
			"updated_record_count": updateRecCount,
		})
	})

	return router
}

func getPaginationParams(params map[string][]string) (int, int, error) {
	var limit, offset int
	var err error

	// page limit
	if vals, prs := params["limit"]; !prs {
		limit = -1
	} else {
		if len(vals) > 1 {
			return limit, offset, errors.New("can only provide 1 value for parameter 'limit'")
		}
		limit, err = strconv.Atoi(vals[0])
		if err != nil {
			return limit, offset, err
		}
		if limit <= 0 {
			return offset, offset, errors.New("limit must be > 0")
		}
		fmt.Println(limit)
	}

	// page offset
	if vals, prs := params["offset"]; !prs {
		offset = 0
	} else {
		if len(vals) > 1 {
			return offset, offset, errors.New("can only provide 1 value for parameter 'offset'")
		}
		offset, err = strconv.Atoi(vals[0])
		if err != nil {
			return offset, offset, err
		}
		if offset < 0 {
			return offset, offset, errors.New("offset must be >= 0")
		}
	}

	return limit, offset, nil
}
