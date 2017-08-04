package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/smallfish/simpleyaml"
	"github.com/spf13/afero"
	"io/ioutil"
	"log"
	"menteslibres.net/gosexy/to"
	"os"
	"os/user"
)

// Gets the full path of the user's home directory
func getUserHome() string {
	usr, err := user.Current()
	if err != nil {
		log.Print("Couldn't get user's home directory.")
		log.Fatal(err)
	}
	return usr.HomeDir
}

// Loads the configuration file and starts db connection.
func setupConfig(ctx *Context) error {
	loadConfigFile(ctx)

	ctx.UserHome = getUserHome()
	ctx.os = afero.NewOsFs() // Interface for file system
	ctx.Temp = ctx.UserHome + "/temp"
	err := ctx.os.MkdirAll(ctx.Temp, os.ModePerm)
	if err != nil {
		return handle("Error in making temp dir", err)
	}
	ctx.svcS3 = s3.New(session.Must(session.NewSession()))

	if serv := os.Getenv("SERVER"); serv != "" {
		ctx.Server = serv
	}
	if region := os.Getenv("AWS_REGION"); region == "" {
		if err = os.Setenv("AWS_REGION", "us-west-2"); err != nil {
			return handle("Error in setting region", err)
		}
	}
	return err
}

// Loads config details from the config file.
var ioutilReadFile = ioutil.ReadFile

func loadConfigFile(ctx *Context) {
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
		ctx.Server = str
	}
	if str, err = yml.Get("bucket").String(); err != nil {
		log.Fatal("Error in setting bucket. ", err)
	}
	ctx.Bucket = str

	loadSyncFolders(ctx, yml)
}

func loadSyncFolders(ctx *Context, yml *simpleyaml.Yaml) {
	// Load sync folder details
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
