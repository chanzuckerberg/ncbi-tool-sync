package models

import (
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws"
    "time"
    "github.com/aws/aws-sdk-go/service/s3"
    "ncbi_proj/server/utils"
    "strconv"
    "fmt"
    "database/sql"
)

type File struct {
}

type Response struct {
    Path    string
    ModTime string
    Url     string
}

func NewFile() *File {
    return &File{}
}

func (f *File) Get(pathName string, versionNum string, ctx *utils.Context) (Response, error) {
    var url string
    var err error
    key := pathName
    resp := Response{}

    // Get archive blob key if version specified
    if versionNum != "" {
        num, _ := strconv.Atoi(versionNum)
        key, err = f.getS3Key(pathName, num, ctx)
        if err != nil {
            return resp, err
        }
    }

    url, err = f.S3KeyToURL(key, ctx)
    if err == nil {
        resp.Path = pathName
        resp.ModTime = "Placeholder"
        resp.Url = url
    }
    return resp, err
}

// Look in database for proper key for specific version
func (f *File) getS3Key(pathName string, versionNum int, ctx *utils.Context) (string, error) {
    // Query the database
    res := ""
    query := fmt.Sprintf("select * from entries " +
        "where PathName='%s' and VersionNum=%d", pathName, versionNum)
    row, err := ctx.Db.Query(query)
    defer row.Close()
    if err != nil {
        return res, err
    }

    // Process results
    row.Next()
    var path string;
    var version int;
    var date sql.NullString;
    var blob sql.NullString;

    err = row.Scan(&path, &version, &date, &blob)
    if err != nil {
        // No results at all for this name and version
        return res, err
    } else if !blob.Valid {
        // Entry is there but not archived. Serve latest.
        return pathName, nil
    } else {
        // Success
        archiveKey, _ := blob.Value()
        res = fmt.Sprintf("/archive/%s", archiveKey)
    }
    return res, err
}

// Get a pre-signed temporary URL from S3 for a key
func (f *File) S3KeyToURL(key string, ctx *utils.Context) (string, error) {
    url := ""
    svc := s3.New(session.New(&aws.Config{
        Region: aws.String("us-west-2"),
    }))
    req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
        Bucket: aws.String(ctx.Bucket),
        Key:    aws.String(key),
    })
    url, err := req.Presign(1 * time.Hour)
    return url, err
}

func (f *File) GetHistory(pathName string) {

}