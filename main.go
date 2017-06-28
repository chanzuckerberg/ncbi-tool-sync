package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"log"
	"os"
)

// Context holds application state variables
type Context struct {
	Db         *sql.DB
	os         afero.Fs
	Server     string `yaml:"Server"`
	Port       string `yaml:"Port"`
	Username   string `yaml:"Username"`
	Password   string `yaml:"Password"`
	SourcePath string `yaml:"SourcePath"`
	LocalPath  string `yaml:"LocalPath"`
	LocalTop   string `yaml:"LocalTop"`
	Bucket     string `yaml:"Bucket"`
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
	ctx.loadConfig()
	ctx.SetupDatabase()
	defer ctx.Db.Close()

	// Mount FUSE directory
	//ctx.UnmountFuse()
	err = ctx.MountFuse()
	//defer ctx.UmountFuse()
	if err != nil {
		log.Fatal(err)
	}

	ctx.ingestCurrentFiles()
	return

	// Call Rsync flow
	err = ctx.callRsyncFlow()
	if err != nil {
		log.Fatal(err)
	}
}
