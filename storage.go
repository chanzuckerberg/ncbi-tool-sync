package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
	"strings"
)

// MountFuse mounts the virtual directory. Uses goofys tool to mount
// S3 as a local folder for syncing operations.
func (ctx *Context) MountFuse() error {
	log.Println("Starting FUSE mount...")
	_ = ctx.os.Mkdir(ctx.LocalTop, os.ModePerm)
	goofys := os.Getenv("GOOFYS")
	cmd := fmt.Sprintf("./goofys %s %s", ctx.Bucket, ctx.LocalTop)
	if goofys != "" {
		cmd = fmt.Sprintf("%s %s %s", goofys, ctx.Bucket,
			ctx.LocalTop)
	}
	_, _, err := commandVerbose(cmd)
	if err != nil {
		log.Println("Error in mounting FUSE.")
		return err
	}
	time.Sleep(time.Duration(3)*time.Second)
	return err
}

// UnmountFuse unmounts the virtual directory. Ignores errors since
// directory may already be unmounted.
func (ctx *Context) UnmountFuse() {
	commandVerbose("sudo umount -f " + ctx.LocalTop)
	time.Sleep(time.Duration(3)*time.Second)
}

func (ctx *Context) checkMount() {
	_, stderr, err := commandVerbose("ls " + ctx.LocalTop)
	if strings.Contains(stderr, "Transport endpoint is not connected") || err != nil {
		log.Fatal("Can't connect to mount point.")
	}
	log.Println("Mount check successful.")
}

func (ctx *Context) checkMountRepeat(quit chan bool) {
	go func() {
		for {
			select {
			case <- quit:
				return
			default:
				stdout, stderr, err := commandWithOutput("ls " + ctx.LocalTop)
				if strings.Contains(stderr, "endpoint is not connected") || strings.Contains(stderr, "is not a mountpoint") || err != nil {
					log.Println(stdout)
					log.Println(stderr)
					log.Println("Can't connect to mount point.")
					ctx.UnmountFuse()
					ctx.MountFuse()
				}
				time.Sleep(time.Duration(5)*time.Second)
			}
		}
	}()
}


// SetupDatabase sets up the db and checks connection conditions
func (ctx *Context) SetupDatabase() {
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
		log.Println("DB connection string: " + sourceName)
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

// CreateTable creates the table and schema in the db if needed.
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
