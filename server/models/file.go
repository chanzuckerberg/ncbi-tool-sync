package models

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"ncbi_proj/server/utils"
	"strconv"
	"time"
)

type File struct {
	ctx *utils.Context
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

type Entry struct {
	Path    string  `json:",omitempty"`
	Version int     `json:",omitempty"`
	ModTime string  `json:",omitempty"`
	Url     string  `json:",omitempty"`
}

// Get response for a file and version
func (f *File) GetVersion(pathName string,
	versionNum string) (Entry, error) {
	return f.Get(pathName, "versionNum", versionNum)
}

// Get response for file, latest version
func (f *File) GetLatest(pathName string) (Entry, error) {
	return f.Get(pathName, "latest", "")
}

// Get the file as it existed at/before the given time
func (f *File) GetAtTime(pathName string,
	inputTime string) (Entry, error) {
	return f.Get(pathName, "inputTime", inputTime)
}

// General get handler
func (f *File) Get(pathName string, attribute string,
	val string) (Entry, error) {
	key := pathName
	info := Metadata{}
	res := Entry{}
	var err error

	switch attribute {
	case "latest":
		info, err = f.entryFromVersion(pathName, 0)
	case "versionNum":
		num, _ := strconv.Atoi(val)
		info, err = f.entryFromVersion(pathName, num)
	case "inputTime":
		info, err = f.entryFromTime(pathName, val)
	default:
		return res, err
	}
	if err != nil {
		return res, err
	}

	key = f.getS3Key(info)
	url, err := f.S3KeyToURL(key)
	if err != nil {
		return res, err
	}
	return Entry{
		info.Path,
		info.Version,
		info.ModTime.String,
		url}, err
}

// Get metadata based on name and given time
func (f *File) entryFromTime(pathName string,
	inputTime string) (Metadata, error) {
	// Query the database
	query := fmt.Sprintf("select * from entries where "+
		"PathName='%s' and DateModified <= datetime('%s') order "+
		"by VersionNum desc", pathName, inputTime)
	return f.topFromQuery(query)
}

// Get column info from the top db result of the query
func (f *File) topFromQuery(query string) (Metadata, error) {
	md := Metadata{}
	row, err := f.ctx.Db.Query(query)
	defer row.Close()
	if err != nil {
		return md, err
	}

	// Process results
	present := row.Next()
	if !present {
		return md, errors.New("No results for this query.")
	}
	err = row.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	return md, err
}

// Get info about the file from the db
func (f *File) entryFromVersion(pathName string,
	versionNum int) (Metadata, error) {
	query := ""
	if versionNum > 0 {
		// Get specified version
		query = fmt.Sprintf("select * from entries "+
			"where PathName='%s' and VersionNum=%d", pathName, versionNum)
	} else {
		// Get latest version
		query = fmt.Sprintf("select * from entries "+
			"where PathName='%s' order by VersionNum desc", pathName)
	}
	return f.topFromQuery(query)
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
	req, _ := f.ctx.Store.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(f.ctx.Bucket),
		Key:    aws.String(key),
	})
	url, err := req.Presign(1 * time.Hour)
	return url, err
}

// Get response for the revision history of a file
func (f *File) GetHistory(pathName string) ([]Entry, error) {
	var err error
	res := []Entry{}

	// Query the database
	query := fmt.Sprintf("select * from entries "+
		"where PathName='%s' order by VersionNum desc", pathName)
	rows, err := f.ctx.Db.Query(query)
	defer rows.Close()
	if err != nil {
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
		entry := Entry{
			Path: md.Path,
			Version: md.Version,
			ModTime: md.ModTime.String,
		}
		res = append(res, entry)
	}

	return res, err
}
