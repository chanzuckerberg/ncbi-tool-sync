package storage

import (
    "fmt"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws"
    "os"
    "bytes"
    "net/http"
    "github.com/aws/aws-sdk-go/aws/awsutil"
    "github.com/aws/aws-sdk-go/aws/endpoints"
    "github.com/aws/aws-sdk-go/service/s3"
)

type Context struct {
    LocalPath  string `yaml:"LocalPath"`
    LocalTop   string `yaml:"LocalTop"`
    Bucket     string `yaml:"Bucket"`
}

// Load the configuration file
func (c *Context) LoadConfig() *Context {
    file, err := ioutil.ReadFile("config.yaml")
    if err != nil { panic(err) }

    err = yaml.Unmarshal(file, c)
    if err != nil { panic(err) }

    fmt.Println(c.Bucket)

    return c
}

// Puts a file from a local path to a remote path on S3
func (c *Context) PutFile(localPath string, remotePath string) error {
    // Connect to S3
    sess := session.Must(session.NewSession(&aws.Config{
        Region: aws.String(endpoints.UsWest2RegionID),
    }))
    svc := s3.New(sess)

    // Prepare object details to send
    file, err := os.Open(localPath)
    if err != nil { return err }
    defer file.Close()

    fileInfo, _ := file.Stat()
    size := fileInfo.Size()
    buffer := make([]byte, size)

    file.Read(buffer)
    fileBytes := bytes.NewReader(buffer)
    fileType := http.DetectContentType(buffer)
    params := &s3.PutObjectInput{
        Bucket: aws.String(c.Bucket),
        Key: aws.String(remotePath),
        Body: fileBytes,
        ContentLength: aws.Int64(size),
        ContentType: aws.String(fileType),
    }
    resp, err := svc.PutObject(params)
    if err != nil { return err }
    fmt.Printf("response %s", awsutil.StringValue(resp))

    return err
}
