package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"log"
	"os"
	"io"
	"github.com/jasonlvhit/gocron"
)

// Context holds application state variables
type Context struct {
	Db         *sql.DB
	os         afero.Fs
	Server     string `yaml:"Server"`
	SourcePath string `yaml:"SourcePath"`
	Bucket     string `yaml:"Bucket"`
	LocalPath  string // Ex: $HOME/remote/blast/db
	LocalTop   string // Set as $HOME/remote
	Archive    string // Set as $HOME/remote/archive
	UserHome   string
	TempNew	   string // Set as $HOME/tempNew
	TempOld	   string // Set as $HOME/tempOld
}

// Entry point for the entire sync workflow with remote server.
func main() {
	// Set up logging
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logFile, err := os.OpenFile("log.txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Couldn't open log file.")
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	// General config
	ctx := Context{}
	ctx.UserHome = getUserHome()
	ctx.setupConfig()
	ctx.SetupDatabase()
	defer ctx.Db.Close()

	// Run immediately to start with
	ctx.callSyncFlow()

	// Schedule task to run periodically
	gocron.Every(2).Hours().Do(ctx.callSyncFlow)
	log.Print("Task has been scheduled...")
	<- gocron.Start()
}