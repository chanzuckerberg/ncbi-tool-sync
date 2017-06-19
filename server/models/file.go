package models

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
	"strconv"
	"time"
)

type File struct {
}

type Meta struct {
	pathName     string
	versionNum   int
	dateModified sql.NullString
	archiveKey   sql.NullString
}

type Response struct {
	Path    string
	ModTime string
	Url     string
}

type HistoryResponse struct {
	Path    string
	Version int
	ModTime string
}

// Get response for a file and version
func (f *File) Get(pathName string, versionNum string, ctx *utils.Context) (Response, error) {
	var url string
	var err error
	key := pathName
	resp := Response{}

	// Get file info from db
	num, _ := strconv.Atoi(versionNum)
	info, err := f.getDbInfo(pathName, num, ctx)
	if err != nil {
		// No results at all for this name and version
		return resp, err
	}

	// Get archive blob key if version is specified.
	// Otherwise leave plain pathName for latest version.
	if versionNum != "" {
		key = f.getS3Key(info, ctx)
	}

	url, err = f.S3KeyToURL(key, ctx)
	if err == nil {
		resp.Path = pathName
		resp.ModTime = info.dateModified.String
		resp.Url = url
	}
	return resp, err
}

// Get info about the file from the db
func (f *File) getDbInfo(pathName string, versionNum int, ctx *utils.Context) (Meta, error) {
	// Query the database
	md := Meta{}
	query := fmt.Sprintf("select * from entries "+
		"where PathName='%s' and VersionNum=%d", pathName, versionNum)
	row, err := ctx.Db.Query(query)
	defer row.Close()
	if err != nil {
		return md, err
	}

	// Process results
	row.Next()
	err = row.Scan(&md.pathName, &md.versionNum, &md.dateModified, &md.archiveKey)
	return md, err
}

// Look in database for proper key for specific version
func (f *File) getS3Key(info Meta, ctx *utils.Context) string {
	res := ""
	if !info.archiveKey.Valid {
		// Entry is there but not archived. Just serve the latest.
		return info.pathName
	} else {
		// Success
		archiveKey := info.archiveKey.String
		res = fmt.Sprintf("/archive/%s", archiveKey)
	}
	return res
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

// Get response for the revision history of a file
func (f *File) GetHistory(pathName string,
	ctx *utils.Context) ([]HistoryResponse, error) {
	var err error
	res := []HistoryResponse{}

	// Query the database
	query := fmt.Sprintf("select * from entries "+
		"where PathName='%s' order by VersionNum desc", pathName)
	rows, err := ctx.Db.Query(query)
	defer rows.Close()
	if err != nil {
		// Unsuccessful db query
		return res, err
	}

	// Process results
	md := Meta{}
	for rows.Next() {
		err = rows.Scan(&md.pathName, &md.versionNum,
			&md.dateModified, &md.archiveKey)
		if err != nil {
			return res, err
		}
		entry := HistoryResponse{
			md.pathName,
			md.versionNum,
			md.dateModified.String,
		}
		res = append(res, entry)
	}

	return res, err
}
