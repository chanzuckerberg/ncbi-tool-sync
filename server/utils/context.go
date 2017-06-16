package utils

import (
	"database/sql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Context struct {
	Db         *sql.DB
	Server     string `yaml:"Server"`
	Port       string `yaml:"Port"`
	Username   string `yaml:"Username"`
	Password   string `yaml:"Password"`
	SourcePath string `yaml:"SourcePath"`
	LocalPath  string `yaml:"LocalPath"`
	LocalTop   string `yaml:"LocalTop"`
	Bucket     string `yaml:"Bucket"`
}

func NewContext() *Context {
	ctx := Context{}
	ctx.loadConfig()
	return &ctx
}

// Load the configuration file
func (ctx *Context) loadConfig() *Context {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(file, ctx)
	if err != nil {
		panic(err)
	}

	return ctx
}
