package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

// setupDatabase sets up the db and checks connection conditions
func setupDatabase(ctx *Context) {
	var err error
	isDevelopment := os.Getenv("ENVIRONMENT") == "development"
	if isDevelopment {
		ctx.Db, err = sql.Open("mysql",
			"dev:password@tcp(127.0.0.1:3306)/testdb")
	} else {
		// Setup RDS db from env variables
		rdsHostname := os.Getenv("RDS_HOSTNAME")
		rdsPort := os.Getenv("RDS_PORT")
		rdsDbName := os.Getenv("RDS_DB_NAME")
		rdsUsername := os.Getenv("RDS_USERNAME")
		rdsPassword := os.Getenv("RDS_PASSWORD")
		sourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			rdsUsername, rdsPassword, rdsHostname, rdsPort, rdsDbName)
		log.Print("DB connection string: " + sourceName)
		ctx.Db, err = sql.Open("mysql", sourceName)
	}

	if err != nil {
		log.Print(err)
		log.Fatal("Failed to set up database opener.")
	}
	err = ctx.Db.Ping()
	if err != nil {
		log.Print(err)
		log.Fatal("Failed to ping database.")
	}
	CreateTable(ctx)
	log.Print("Successfully checked database.")
}

// CreateTable creates the table and schema in the db if needed.
func CreateTable(ctx *Context) {
	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(500) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified DATETIME, " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	_, err := ctx.Db.Exec(query)
	if err != nil {
		log.Print(err)
		log.Fatal("Failed to find or create table.")
	}
}
