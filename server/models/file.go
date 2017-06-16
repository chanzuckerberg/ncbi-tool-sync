package models

import (
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws"
    "time"
    "github.com/aws/aws-sdk-go/service/s3"
    "ncbi_proj/server/utils"
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

func (f *File) Get(pathName string, versionNum int, ctx *utils.Context) (Response, error) {
    url, err := f.S3KeyToURL(pathName, ctx)
    if err != nil {
        return Response{}, err
    }
    return Response{pathName, "Placeholder", url}, nil
}

func (f *File) S3KeyToURL(pathName string, ctx *utils.Context) (string, error) {
    // Get a presigned temporary URL from S3 for a key
    svc := s3.New(session.New(&aws.Config{
        Region: aws.String("us-west-2"),
    }))
    req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
        Bucket: aws.String(ctx.Bucket),
        Key:    aws.String(pathName),
    })
    url, err := req.Presign(1 * time.Hour)

    if err != nil {
        return "", err
    }

    return url, err
}

func (f *File) GetLatest(pathName string) {

}

func (f *File) GetHistory(pathName string) {

}