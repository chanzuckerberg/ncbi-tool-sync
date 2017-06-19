package models

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
	"strconv"
	"time"
)

type File struct {
	ctx   *utils.Context
}

func NewFile(ctx *utils.Context) *File {
	return &File{
		ctx: ctx,
	}
}

type Metadata struct {
	Path       string
	Version    int
	ModTime    sql.NullString
	ArchiveKey sql.NullString
}

type EntryWithUrl struct {
	VersionEntry
	Url string
}

type VersionEntry struct {
	Path    string
	Version int
	ModTime string
}

// Get response for a file and version
func (f *File) Get(pathName string, versionNum string) (EntryWithUrl, error) {
	var url string
	var err error
	key := pathName
	resp := EntryWithUrl{}

	// Get file info from db
	num, _ := strconv.Atoi(versionNum)
	info, err := f.getDbInfo(pathName, num)
	if err != nil {
		// No results at all for this name and version
		err = errors.New("No results for this file and version.")
		return resp, err
	}

	// Get archive blob key if version is specified.
	// Otherwise leave plain pathName for latest version.
	if versionNum != "" {
		key = f.getS3Key(info)
	}

	url, err = f.S3KeyToURL(key)
	if err == nil {
		resp.Path = info.Path
		resp.Version = info.Version
		resp.ModTime = info.ModTime.String
		resp.Url = url
	}
	return resp, err
}

// Get info about the file from the db
func (f *File) getDbInfo(pathName string, versionNum int) (Metadata, error) {
	// Query the database
	md := Metadata{}
	var query string

	if versionNum > 1 {
		// Get specified version
		query = fmt.Sprintf("select * from entries "+
			"where PathName='%s' and VersionNum=%d", pathName, versionNum)
	} else {
		// Get latest version
		query = fmt.Sprintf("select * from entries "+
			"where PathName='%s' order by VersionNum desc", pathName)
	}

	row, err := f.ctx.Db.Query(query)
	defer row.Close()
	if err != nil {
		return md, err
	}

	// Process results
	row.Next()
	err = row.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	return md, err
}

// Look in database for proper key for specific version
func (f *File) getS3Key(info Metadata) string {
	res := ""
	if !info.ArchiveKey.Valid {
		// VersionEntry is there but not archived. Just serve the latest.
		return info.Path
	} else {
		// Success
		archiveKey := info.ArchiveKey.String
		res = fmt.Sprintf("/archive/%s", archiveKey)
	}
	return res
}

// Get a pre-signed temporary URL from S3 for a key
func (f *File) S3KeyToURL(key string) (string, error) {
	url := ""
	svc := s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})))
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(f.ctx.Bucket),
		Key:    aws.String(key),
	})
	url, err := req.Presign(1 * time.Hour)
	return url, err
}

// Get response for the revision history of a file
func (f *File) GetHistory(pathName string) ([]VersionEntry, error) {
	var err error
	res := []VersionEntry{}

	// Query the database
	query := fmt.Sprintf("select * from entries "+
		"where PathName='%s' order by VersionNum desc", pathName)
	rows, err := f.ctx.Db.Query(query)
	defer rows.Close()
	if err != nil {
		// Unsuccessful db query
		return res, err
	}

	// Process results
	md := Metadata{}
	for rows.Next() {
		err = rows.Scan(&md.Path, &md.Version,
			&md.ModTime, &md.ArchiveKey)
		if err != nil {
			return res, err
		}
		entry := VersionEntry{
			md.Path,
			md.Version,
			md.ModTime.String,
		}
		res = append(res, entry)
	}

	return res, err
}
