package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"log"
	"os"
	"io"
	"strings"
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
	TempNew	   string
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func test() {
	ctx := Context{}
	//var err error

	// General config
	ctx.UserHome = getUserHome()
	ctx.setupConfig()
	ctx.SetupDatabase()
	defer ctx.Db.Close()
}

// Entry point for the entire sync workflow with remote server.
func main() {
	logFile, err := os.OpenFile("log.txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Couldn't open log file.")
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	ctx := Context{}
	//var err error

	// General config
	ctx.UserHome = getUserHome()
	ctx.setupConfig()
	ctx.SetupDatabase()
	defer ctx.Db.Close()

	// Mount FUSE directory
	//ctx.UnmountFuse()
	//err = ctx.MountFuse()
	//defer ctx.UmountFuse()
	//if err != nil {
	//	log.Println(err.Error())
	//}

	ctx.callRsyncFlow()

	// Schedule task to run every day at 3 a.m.
	//gocron.Every(1).Day().At("03:00").Do(ctx.callRsyncFlow)
	gocron.Every(3).Hours().Do(ctx.callRsyncFlow)
	log.Println("Task has been scheduled...")
	<- gocron.Start()
}

//func (ctx *Context) checkMountRoutine() {
//	for {
//		stdout, stderr, err := commandWithOutput("ls " + ctx.LocalTop)
//		if strings.Contains(stderr, "Transport endpoint is not connected") || err != nil {
//			log.Println(stdout)
//			log.Println(stderr)
//			log.Println("Can't connect to mount point.")
//			ctx.UnmountFuse()
//			ctx.MountFuse()
//		}
//		stdout, stderr, err = commandWithOutput("mountpoint " + ctx.LocalTop)
//		if strings.Contains(stderr, "is not a mountpoint") || err != nil {
//			log.Println(stdout)
//			log.Println(stderr)
//			log.Println("Can't connect to mount point.")
//		}
//		log.Println("Mount check successful.")
//		time.Sleep(time.Duration(30)*time.Second)
//	}
//}

func (ctx *Context) checkMount() {
	_, stderr, err := commandVerbose("ls " + ctx.LocalTop)
	if strings.Contains(stderr, "Transport endpoint is not connected") || err != nil {
		log.Fatal("Can't connect to mount point.")
	}
	//_, stderr, err = commandVerbose("mountpoint " + ctx.LocalTop)
	//if strings.Contains(stderr, "is not a mountpoint") || err != nil {
	//	log.Fatal("Can't connect to mount point.")
	//}
	log.Println("Mount check successful.")
}