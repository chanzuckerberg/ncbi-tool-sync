package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"log"
	"os"
	//"github.com/jasonlvhit/gocron"
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
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Entry point for the entire sync workflow with remote server.
func main() {
	ctx := Context{}
	var err error

	// General config
	ctx.UserHome = getUserHome()
	ctx.setupConfig()
	ctx.SetupDatabase()
	defer ctx.Db.Close()

	// Mount FUSE directory
	//ctx.UnmountFuse()
	err = ctx.MountFuse()
	//defer ctx.UmountFuse()
	if err != nil {
		log.Println(err.Error())
	}

	ctx.callRsyncFlow()

	// Schedule task to run every day at 3 a.m.
	//gocron.Every(1).Day().At("03:00").Do(ctx.callRsyncFlow)
	//<- gocron.Start()
}
