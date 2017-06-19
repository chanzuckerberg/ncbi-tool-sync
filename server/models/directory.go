package models

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
)

type Directory struct {
}

type PathEntry struct {
	Path string
}

type PathUrlEntry struct {
	Path string
	Url  string
}

// Get directory listing paths-only, latest versions
func (d *Directory) Get(pathName string,
	ctx *utils.Context) ([]PathEntry, error) {
	// Setup
	var err error
	resp := []PathEntry{}

	// List objects at the path
	listing, err := d.ListObjects(pathName, ctx)
	if err != nil || len(listing) < 1 {
		return resp, errors.New("Directory does not exist")
	}

	// Process results
	for _, val := range listing {
		entry := PathEntry{*val.Key}
		resp = append(resp, entry)
	}

	return resp, err
}

// Get directory listing with download URLs, latest versions
func (d *Directory) GetWithURLs(pathName string,
	ctx *utils.Context) ([]PathUrlEntry, error) {
	// Setup
	resp := []PathUrlEntry{}
	var err error
	file := new(File)

	// List objects at the path
	listing, err := d.ListObjects(pathName, ctx)
	if err != nil || len(listing) < 1 {
		return resp, errors.New("Directory does not exist")
	}

	// Process results
	for _, val := range listing {
		key := *val.Key
		url, err := file.S3KeyToURL(key, ctx)
		if err != nil {
			return resp, err
		}
		entry := PathUrlEntry{key, url}
		resp = append(resp, entry)
	}

	return resp, err
}

// Check if a file exists on S3
func (d *Directory) FileExists(pathName string,
	ctx *utils.Context) (bool, error) {
	svc := s3.New(session.New(),
		&aws.Config{Region: aws.String("us-west-2")})
	params := &s3.HeadObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(pathName),
	}
	_, err := svc.HeadObject(params)
	if err != nil {
		return false, err
	}
	return true, err
}

// List objects with a given prefix in S3
func (d *Directory) ListObjects(pathName string,
	ctx *utils.Context) ([]*s3.Object, error) {
	var err error

	pathName = pathName[1:]
	svc := s3.New(session.New(),
		&aws.Config{Region: aws.String("us-west-2")})
	params := &s3.ListObjectsInput{
		Bucket: aws.String(ctx.Bucket),
		Prefix: aws.String(pathName),
	}

	temp, err := svc.ListObjects(params)
	res := temp.Contents
	return res, err
}
