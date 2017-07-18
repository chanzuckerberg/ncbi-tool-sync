package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// MountFuse mounts the virtual directory. Uses goofys tool to mount
// S3 as a local folder for syncing operations.
func MountFuse(ctx *Context) error {
	log.Print("Starting FUSE mount...")
	_ = ctx.os.Mkdir(ctx.LocalTop, os.ModePerm)
	goofys := os.Getenv("GOOFYS")
	cmd := fmt.Sprintf("./goofys %s %s", ctx.Bucket, ctx.LocalTop)
	if goofys != "" {
		cmd = fmt.Sprintf("%s %s %s", goofys, ctx.Bucket,
			ctx.LocalTop)
	}
	_, _, err := commandVerboseOnErr(cmd)
	if err != nil {
		err = newErr("Error in mounting FUSE.", err)
		log.Print(err)
		return err
	}
	time.Sleep(time.Duration(3) * time.Second)
	return err
}

// UnmountFuse unmounts the virtual directory. Ignores errors since
// directory may already be unmounted.
func UnmountFuse(ctx *Context) {
	commandVerboseOnErr("sudo umount " + ctx.LocalTop)
	time.Sleep(time.Duration(3) * time.Second)
}

// checkMount checks if the FUSE endpoint is connected.
func checkMount(ctx *Context) {
	cmd := "ls "
	if runtime.GOOS == "linux" {
		cmd = "mountpoint "
	}
	_, stderr, err := commandVerboseOnErr(cmd + ctx.LocalTop)
	if strings.Contains(stderr, "Transport endpoint is not "+
		"connected") || strings.Contains(stderr, "is not a "+
		"mountpoint") || err != nil {
		log.Fatal("Can't connect to mount point.")
	}
	log.Print("Mount check successful.")
}

// Starts goroutine for checking the FUSE connection continuously and trying
// to reconnect.
func checkMountRepeat(ctx *Context, quit chan bool) {
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				cmd := "ls "
				if runtime.GOOS == "linux" {
					cmd = "mountpoint "
				}
				stdout, stderr, err := commandWithOutput(cmd + ctx.LocalTop)
				if strings.Contains(stderr, "endpoint is not "+
					"connected") || strings.Contains(stderr, "is not "+
					"a mountpoint") || err != nil {
					log.Print(stdout)
					log.Print(stderr)
					log.Print("Can't connect to mount point.")
					UnmountFuse(ctx)
					MountFuse(ctx)
				}
				time.Sleep(time.Duration(5) * time.Second)
			}
		}
	}()
}

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
