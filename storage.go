package main

import (
	"fmt"
	"log"
	"os"
	"database/sql"
)

// Mounts the virtual directory. Uses goofys tool to mount S3 as a
// local folder for syncing operations.
func (ctx *Context) MountFuse() error {
	log.Println("Starting FUSE mount...")
	_ = ctx.os.Mkdir("remote", os.ModePerm)
	isDevelopment := os.Getenv("ENVIRONMENT") == "development"
	cmd := fmt.Sprintf("./goofys %s remote", ctx.Bucket)
	if isDevelopment {
		cmd = fmt.Sprintf("./goofys-mac %s remote", ctx.Bucket)
	}
	out, err := callCommand(cmd)
	if err != nil {
		log.Println(out)
		log.Println(err.Error())
		log.Fatal("Error in mounting FUSE.")
	}
	log.Println("Successful FUSE mount.")
	return err
}

// Unmounts the virtual directory. Ignores errors since directory may
// already be unmounted.
func (ctx *Context) UnmountFuse() {
	cmd := fmt.Sprintf("umount remote")
	callCommand(cmd)
}

func (ctx *Context) SetupDatabase() {
	var err error
	isDevelopment := os.Getenv("ENVIRONMENT") == "development"
	if isDevelopment {
		ctx.Db, err = sql.Open("mysql",
			"dev:password@tcp(127.0.0.1:3306)/testdb")
	} else {
		// Setup RDS db from env variables
		RDS_HOSTNAME := os.Getenv("RDS_HOSTNAME")
		RDS_PORT := os.Getenv("RDS_PORT")
		RDS_DB_NAME := os.Getenv("RDS_DB_NAME")
		RDS_USERNAME := os.Getenv("RDS_USERNAME")
		RDS_PASSWORD := os.Getenv("RDS_PASSWORD")
		sourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			RDS_USERNAME, RDS_PASSWORD, RDS_HOSTNAME, RDS_PORT, RDS_DB_NAME)
		log.Println("RDS connection string: " + sourceName)
		ctx.Db, err = sql.Open("mysql", sourceName)
	}

	if err != nil {
		log.Println(err)
		log.Fatal("Failed to set up database opener.")
	}
	err = ctx.Db.Ping()
	if err != nil {
		log.Println(err)
		log.Fatal("Failed to ping database.")
	}
	ctx.CreateTable()
	log.Println("Successfully connected database.")
}

// Creates the table and schema in the db if needed.
func (ctx *Context) CreateTable() {
	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(500) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified DATETIME, " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	_, err := ctx.Db.Exec(query)
	if err != nil {
		log.Println(err)
		log.Fatal("Failed to find or create table.")
	}
}