package main

import (
	"database/sql"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
	"io"
	"log"
	"os"
)

// A context holds application state variables.
type context struct {
	db          *sql.DB
	os          afero.Fs
	server      string       `yaml:"server"`
	bucket      string       `yaml:"bucket"`
	syncFolders []syncFolder `yaml:"syncFolders"`
	local       string // Set as /syncmount
	temp        string // Set as /syncmount/synctemp
	svcS3       *s3.S3
}

// A syncFolder represents a folder path to sync and rsync flags as strings.
type syncFolder struct {
	sourcePath string
	flags      []string
}

// Entry point for the entire sync workflow with remote server.
func main() {
	// Set up logging
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logFile, err := os.OpenFile("log.txt",
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Couldn't open log file.")
	}
	defer func() {
		err = logFile.Close()
		if err != nil {
			log.Print("Log file was not closed properly. ", err)
		}
	}()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	// General config
	ctx := context{}
	if err = setupConfig(&ctx); err != nil {
		log.Fatal("Error in setting up configuration: ", err)
	}
	if _, err = setupDatabase(&ctx); err != nil {
		log.Fatal("Error in db setup: ", err)
	}
	defer func() {
		err = ctx.db.Close()
		if err != nil {
			log.Print("db was not closed properly. ", err)
		}
	}()

	// Run immediately to start with. Next run is scheduled after completion.
	if err = callSyncFlow(&ctx, true); err != nil {
		errOut("Error in calling sync flow", err)
	}
}
