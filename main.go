package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"io"
	"log"
	"os"
)

// Context holds application state variables
type Context struct {
	Db         *sql.DB
	os         afero.Fs
	Server     string `yaml:"server"`
	Bucket     string `yaml:"bucket"`
	syncFolders []syncFolder `yaml:"syncFolders"`
	LocalTop   string // Set as $HOME/remote
	UserHome   string
	TempNew    string // Set as $HOME/tempNew
}

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
	ctx := Context{}
	ctx.UserHome = getUserHome()
	setupConfig(&ctx)
	setupDatabase(&ctx)
	defer func() {
		err = ctx.Db.Close()
		if err != nil {
			log.Print("Db was not closed properly. ", err)
		}
	}()

	// Run immediately to start with. Next run is scheduled after completion.
	callSyncFlow(&ctx)
}
