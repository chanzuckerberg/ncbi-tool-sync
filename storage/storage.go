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
    "os/exec"
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

// Mount the s3fs-fuse directory
func (c *Context) MountFuse() error {
    _ = os.Mkdir("remote", os.ModePerm)
    cmd := fmt.Sprintf("s3fs %s remote", c.Bucket)
    out, err := callCommand(cmd)
    fmt.Println("%s%s", out, err)
    return err
}

// Unmount the s3fs-fuse directory
func (c *Context) UmountFuse() error {
    cmd := fmt.Sprintf("umount remote")
    out, err := callCommand(cmd)
    fmt.Println("%s%s", out, err)
    return err
}

func callCommand(input string) ([]byte, error) {
    return exec.Command("sh","-c", input).Output()
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
