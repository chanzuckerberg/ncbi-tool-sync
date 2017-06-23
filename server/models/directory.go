package models

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
)

type Directory struct {
	ctx *utils.Context
}

func NewDirectory(ctx *utils.Context) *Directory {
	return &Directory{
		ctx: ctx,
	}
}

func (d *Directory) GetLatest(pathName string,
	output string) ([]Entry, error) {
	// Setup
	var err error
	resp := []Entry{}
	file := NewFile(d.ctx)
	url := ""

	// Get listing from S3
	listing, err := d.ListObj(pathName)
	if err != nil || len(listing) == 0 {
		return resp, errors.New("Empty or non-existent directory.")
	}
	for _, val := range listing {
		key := *val.Key
		if output == "with-URLs" {
			url, err = file.S3KeyToURL(key)
			if err != nil {
				return resp, err
			}
		}
		entry := Entry{Path: *val.Key, Url: url}
		resp = append(resp, entry)
	}

	if len(resp) == 0 {
		err = errors.New("No results.")
	}
	return resp, err
}

func (d *Directory) GetPast(pathName string, inputTime string,
	output string) ([]Entry, error) {
	// Setup
	var err error
	resp := []Entry{}
	file := NewFile(d.ctx)
	url := ""

	// Get archive versions from DB
	listing, err := d.getAtTimeDb(pathName, inputTime)
	if err != nil || len(listing) == 0 {
		return resp, errors.New("Empty or non-existent directory.")
	}

	for _, val := range listing {
		key := file.getS3Key(val)
		if output == "with-URLs" {
			url, err = file.S3KeyToURL(key)
			if err != nil {
				return resp, err
			}
		}
		entry := Entry{
			Path:    val.Path,
			Version: val.Version,
			ModTime: val.ModTime.String,
			Url:     url,
		}
		resp = append(resp, entry)
	}

	if len(resp) == 0 {
		err = errors.New("No results.")
	}
	return resp, err
}

// Get data from rows in database
func (d *Directory) getAtTimeDb(pathName string,
	inputTime string) ([]Metadata, error) {
	// Query
	res := []Metadata{}
	query := fmt.Sprintf("select e.PathName, e.VersionNum, "+
		"e.DateModified, e.ArchiveKey "+
		"from entries as e "+
		"inner join ( "+
		"select max(VersionNum) VersionNum, PathName "+
		"from entries "+
		"where PathName LIKE '%s%%' "+
		"and DateModified <= datetime('%s') "+
		"group by PathName ) as max "+
		"on max.PathName = e.PathName "+
		"and max.VersionNum = e.VersionNum",
		pathName, inputTime)
	rows, err := d.ctx.Db.Query(query)
	defer rows.Close()
	if err != nil {
		fmt.Println("No results?")
		return res, err
	}

	// Process results
	for rows.Next() {
		md := Metadata{}
		err = rows.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
		if err != nil {
			return res, err
		}
		res = append(res, md)
	}
	return res, err
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

	res, err := d.ctx.Store.ListObjects(params)
	pruned := []*s3.Object{}
	for _, val := range res.Contents {
		if int(*val.Size) > 0 { // Don't include the folder itself
			pruned = append(pruned, val)
		}
	}

	return pruned, err
}
