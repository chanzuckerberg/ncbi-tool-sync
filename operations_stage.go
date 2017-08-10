package main

import (
	"log"
	"fmt"
	"errors"
	"strings"
	"gopkg.in/fatih/set.v0"
	"sort"
)

// fileOperationStage executes the actual file operations on local disk and S3.
func fileOperationStage(ctx *context, res syncResult) {
	log.Print("Beginning file operations stage.")

	log.Print("Going to handle new file operations...")
	newFilesOperations(ctx, res.newF)
	log.Print("Going to handle modified file operations...")
	modifiedFilesOperations(ctx, res.modified)
	log.Print("Going to handle deleted file operations...")
	deletedFilesOperations(ctx, res.deleted)
}

// newFilesOperations executes operations for new files. Copies files from
// remote server and uploads to S3.
func newFilesOperations(ctx *context, newF []string) {
	var err error
	for _, file := range newF {
		if err = copyFileFromRemote(ctx, file); err != nil {
			errOut("Error in copying new file from remote.", err)
			continue
		}
		if err = putObject(ctx, ctx.temp+file, file); err != nil {
			errOut("Error in uploading new file to S3.", err)
		}
	}
}

// deletedFilesOperations executes operations for deleted files. Moves old
// copies to archive and deletes the current copy.
func deletedFilesOperations(ctx *context, newF []string) {
	var err error
	for _, file := range newF {
		if err = moveOldFile(ctx, file); err != nil {
			errOut("Error in moving deleted file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			errOut("Error in deleting file.", err)
		}
	}
}

// modifiedFilesOperations executes a single file at-a-time flow for modified
// files. Copies files from remote, moves old files to archive, deletes
// current copy, and uploads new copy.
func modifiedFilesOperations(ctx *context, modified []string) {
	var err error
	for _, file := range modified {
		if err = copyFileFromRemote(ctx, file); err != nil {
			errOut("Error in copying modified file from remote.", err)
			continue
		}
		if err = moveOldFile(ctx, file); err != nil {
			errOut("Error in moving modified file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			errOut("Error in deleting old modified file copies.", err)
		}
		if err = putObject(ctx, ctx.temp+file, file); err != nil {
			errOut("Error in uploading new version of file to S3.", err)
		}
	}
}

// moveOldFile archives a file by moving it from the current directory to the
// archive folder under an archiveKey name and recording it in the db.
func moveOldFile(ctx *context, file string) error {
	// Setup
	var err error
	log.Print("Archiving old version of: " + file)
	num := lastVersionNum(ctx, file, false)
	if num < 1 {
		err = errors.New("")
		return handle("No previous unarchived version found in db", err)
	}
	key, err := generateChecksum(file, num)
	if err != nil {
		return handle("Error in generating checksum.", err)
	}
	if err = moveOldFileOperations(ctx, file, key); err != nil {
		errOut("Error in operation to move old file to archive.", err)
	}
	if err = moveOldFileDb(ctx, key, file, num); err != nil {
		errOut("Error in updating db entry for old file.", err)
	}
	return err
}

// moveOldFileOperations moves the to-be-archived file on S3 to the archive
// folder under a new file key.
func moveOldFileOperations(ctx *context, file string, key string) error {
	// Move to archive folder
	svc := ctx.svcS3
	// Ex: bucket/remote/blast/db/README
	log.Print("Copy from: " + ctx.bucket + file)
	log.Print("Copy-to key: " + "archive/" + key)

	// Get file size
	size, err := fileSizeOnS3(ctx, file, svc)
	if err != nil {
		return handle("Error in getting file size on S3.", err)
	}

	if size < 4500000000 {
		// Handle via S3 SDK
		err = copyOnS3(ctx, file, key, svc)
		if err != nil {
			errOut("Error in copying file on S3.", err)
		}
	} else {
		log.Print("Large file handling...")
		// Handle via S3 command line tool
		template := "aws s3 mv s3://%s%s s3://%s/archive/%s"
		cmd := fmt.Sprintf(template, ctx.bucket, file, ctx.bucket, key)
		_, _, err = commandVerbose(cmd)
		if err != nil {
			errOut("Error in moving file on S3 via CLI.", err)
		}
	}
	return err
}

// combineNames combines the file names from pastState and newState
// representations. Used to return an overall list of files to compare as
// new, modified, or deleted.
func combineNames(pastState map[string]fInfo,
	newState map[string]fInfo) []string {
	combined := set.New()
	for k := range pastState {
		combined.Add(k)
	}
	for k := range newState {
		combined.Add(k)
	}
	res := []string{}
	for _, v := range combined.List() {
		res = append(res, v.(string))
	}
	sort.Strings(res)
	return res
}

// fileChangeLogic goes through a list of file names and decides if they are
// new on remote, modified, deleted, or unchanged. Uses the pastState and
// newState representations. Returns changes in a syncResult.
func fileChangeLogic(pastState map[string]fInfo, newState map[string]fInfo,
	names []string) syncResult {
	var n, m, d []string // New, modified, deleted
	for _, f := range names {
		past, inPast := pastState[f]
		cur, inCurrent := newState[f]
		if !inPast && inCurrent {
			// If not inPast and inCurrent, file is new on remote.
			n = append(n, f)
		} else if inPast && !inCurrent {
			// If inPast and not inCurrent, file is deleted on remote.
			d = append(d, f)
		} else {
			// If file size has changed, it was modified.
			if past.size != cur.size {
				m = append(m, f)
			} else {
				// Count md5 files as modified if their modTime has changed.
				if strings.Contains(f, ".md5") &&
					past.modTime != cur.modTime {
					m = append(m, f)
				}
			}
		}
	}
	return syncResult{n, m, d}
}
