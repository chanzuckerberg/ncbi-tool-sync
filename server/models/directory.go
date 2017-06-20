package models

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
)

type Directory struct {
	ctx *utils.Context
}

type PathEntry struct {
	Path string
}

type PathUrlEntry struct {
	Path string
	Url  string
}

func NewDirectory(ctx *utils.Context) *Directory {
	return &Directory{
		ctx: ctx,
	}
}

// Get directory listing paths-only, latest versions
func (d *Directory) Get(pathName string) ([]PathEntry, error) {
	// Setup
	var err error
	resp := []PathEntry{}

	// List objects at the path
	listing, err := d.ListObj(pathName)
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
func (d *Directory) GetWithURLs(pathName string) ([]PathUrlEntry, error) {
	// Setup
	resp := []PathUrlEntry{}
	var err error
	file := NewFile(d.ctx)

	// List objects at the path
	listing, err := d.ListObj(pathName)
	if err != nil || len(listing) < 1 {
		return resp, errors.New("Directory does not exist")
	}

	// Process results
	for _, val := range listing {
		key := *val.Key
		url, err := file.S3KeyToURL(key)
		if err != nil {
			return resp, err
		}
		entry := PathUrlEntry{key, url}
		resp = append(resp, entry)
	}

	return resp, err
}

// Check if a file exists on S3
func (d *Directory) FileExists(pathName string) (bool, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(d.ctx.Bucket),
		Key:    aws.String(pathName),
	}
	_, err := d.ctx.Store.HeadObject(params)
	if err != nil {
		return false, err
	}
	return true, err
}

// List objects with a given prefix in S3
func (d *Directory) ListObj(pathName string) ([]*s3.Object, error) {
	var err error

	pathName = pathName[1:] // Remove leading forward slash
	params := &s3.ListObjectsInput{
		Bucket: aws.String(d.ctx.Bucket),
		Prefix: aws.String(pathName),
	}

	temp, err := d.ctx.Store.ListObjects(params)
	res := temp.Contents
	return res, err
}
