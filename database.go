package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

var setupDatabase = setupDatabaseWithCtx

// setupDatabaseWithCtx sets up the database from environment variables and
// checks connection conditions.
func setupDatabaseWithCtx(ctx *context) (string, error) {
	var err error
	// Setup RDS db from env variables
	rdsHostname := os.Getenv("RDS_HOSTNAME")
	rdsPort := os.Getenv("RDS_PORT")
	rdsDbName := os.Getenv("RDS_DB_NAME")
	rdsUsername := os.Getenv("RDS_USERNAME")
	rdsPassword := os.Getenv("RDS_PASSWORD")
	sourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		rdsUsername, rdsPassword, rdsHostname, rdsPort, rdsDbName)
	log.Print("DB connection string: " + sourceName)

	if ctx.db, err = sql.Open("mysql", sourceName); err != nil {
		return sourceName, handle("Failed to set up database opener", err)
	}
	if err = ctx.db.Ping(); err != nil {
		return sourceName, handle("Failed to ping database", err)
	}
	CreateTable(ctx)
	log.Print("Successfully checked database.")
	return sourceName, err
}

// CreateTable creates the table and schema in the db if not present.
func CreateTable(ctx *context) {
	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(500) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified DATETIME, " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	if _, err := ctx.db.Exec(query); err != nil {
		log.Print(err)
		log.Fatal("Failed to find or create table.")
	}
}
