package utils

import (
	"database/sql"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// General state variables for the server
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
	Store      s3iface.S3API
}

func NewContext() *Context {
	ctx := Context{}
	ctx.loadConfig()
	ctx.connectAWS()
	return &ctx
}

// Loads the configuration file.
func (ctx *Context) loadConfig() (*Context, error) {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(file, ctx)
	if err != nil {
		return nil, err
	}

	return ctx, err
}

// Creates a new AWS client session.
func (ctx *Context) connectAWS() *Context {
	ctx.Store = s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})))
	return ctx
}
