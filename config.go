package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/smallfish/simpleyaml"
	"github.com/spf13/afero"
	"io/ioutil"
	"log"
	"github.com/gosexy/to"
	"os"
	"os/user"
)

// setupConfig sets up context variables and connections. context is
// shared throughout program execution.
func setupConfig(ctx *context) error {
	loadConfigFile(ctx)
	var err error

	ctx.os = afero.NewOsFs() // Interface for file system

	// Set the local folder as /syncmount/synctemp, which is the mount point
	// created in the Dockerfile. If this doesn't exist (e.g. local dev), set the
	// folder as the running user's home folder ($HOME/synctemp). Check write
	// privileges.
	ctx.local = "/syncmount"
	if _, err = ctx.os.Stat(ctx.local); err != nil {
		ctx.local = getUserHome()
	}
	ctx.temp = ctx.local + "/synctemp"
	if err = ctx.os.MkdirAll(ctx.temp, os.ModePerm); err != nil {
		msg := "Error in making temp dir. May not have write privileges"
		return handle(msg, err)
	}
	if _, err = ctx.os.Create(ctx.temp + "/testFile"); err != nil {
		msg := "Error in making test file. May not have write privileges"
		return handle(msg, err)
	}

	ctx.svcS3 = s3.New(session.Must(session.NewSession()))

	if serv := os.Getenv("SERVER"); serv != "" {
		ctx.server = serv
	}
	// Set the region as us-west-2 if absent.
	if region := os.Getenv("AWS_REGION"); region == "" {
		if err = os.Setenv("AWS_REGION", "us-west-2"); err != nil {
			return handle("Error in setting region", err)
		}
	}
	return err
}

var ioutilReadFile = ioutil.ReadFile

// loadConfigFile loads config details from the config file.
func loadConfigFile(ctx *context) {
	source, err := ioutilReadFile("config.yaml")
	if err != nil {
		log.Fatal("Error in opening config. ", err)
	}
	yml, err := simpleyaml.NewYaml(source)
	if err != nil {
		log.Fatal("Error in parsing config. ", err)
	}

	var str string
	if str, err = yml.Get("server").String(); err != nil {
		log.Print("No server set in config.yaml. Will try to set from env.")
	} else {
		ctx.server = str
	}
	if str, err = yml.Get("bucket").String(); err != nil {
		log.Fatal("Error in setting bucket. ", err)
	}
	ctx.bucket = str

	loadSyncFolders(ctx, yml)
}

// loadSyncFolders loads the folders to sync and flags from config file.
func loadSyncFolders(ctx *context, yml *simpleyaml.Yaml) {
	size, err := yml.Get("syncFolders").GetArraySize()
	if err != nil {
		log.Fatal("Error in loading syncFolders. ", err)
	}
	for i := 0; i < size; i++ {
		folder := yml.Get("syncFolders").GetIndex(i)
		name, err := folder.Get("name").String()
		if err != nil {
			log.Fatal("Error in loading folder name. ", err)
		}
		flagsYml, err := folder.Get("flags").Array()
		if err != nil {
			log.Fatal("Error in loading sync flags. ", err)
		}
		flags := []string{}
		for _, v := range flagsYml {
			flags = append(flags, to.String(v))
		}
		res := syncFolder{name, flags}
		ctx.syncFolders = append(ctx.syncFolders, res)
	}
}

// getUserHome gets the full path of the user's home directory.
func getUserHome() string {
	usr, err := user.Current()
	if err != nil {
		log.Print("Couldn't get user's home directory.")
		log.Fatal(err)
	}
	return usr.HomeDir
}
