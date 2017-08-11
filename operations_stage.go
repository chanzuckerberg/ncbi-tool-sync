package main

import (
	"errors"
	"log"
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
	cache := make(map[string]map[string]string)
	for _, file := range newF {
		if err = copyFileFromRemote(ctx, file); err != nil {
			errOut("Error in copying new file from remote.", err)
			continue
		}
		if err = putObject(ctx, ctx.temp+file, file); err != nil {
			errOut("Error in uploading new file to S3.", err)
			continue
		}
		if err = dbNewVersion(ctx, file, cache); err != nil {
			errOut("Error in adding new version to db", err)
		}
	}
}

// deletedFilesOperations executes operations for deleted files. Moves old
// copies to archive and deletes the current copy.
func deletedFilesOperations(ctx *context, newF []string) {
	var err error
	for _, file := range newF {
		num := lastVersionNum(ctx, file, false)
		if num < 1 {
			err = errors.New("")
			errOut("No previous unarchived version found in db", err)
		}
		key, err := generateChecksum(file, num)
		if err != nil {
			errOut("Error in generating checksum", err)
		}

		if err = moveObject(ctx, file, key); err != nil {
			errOut("Error in moving deleted file to archive.", err)
		}
		if err = dbArchiveFile(ctx, file, key, num); err != nil {
			errOut("Error in archiving file in db", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			errOut("Error in deleting file.", err)
		}
	}
}

// modifiedFilesOperations calls the operations loop for modified files.
func modifiedFilesOperations(ctx *context, modified []string) {
	cache := make(map[string]map[string]string)
	for _, file := range modified {
		if err := modifiedFileOperations(ctx, file, cache); err != nil {
			errOut("Error in modified file operations", err)
		}
	}
}

// modifiedFileOperations executes a single file at-a-time flow for modified
// files. Copies files from remote, moves old files to archive, deletes
// current copy, uploads new copy, and updates db state.
func modifiedFileOperations(ctx *context, file string,
	cache map[string]map[string]string) error {
	var err error
	if err = copyFileFromRemote(ctx, file); err != nil {
		return handle("Error in copying modified file from remote", err)
	}
	num := lastVersionNum(ctx, file, false)
	if num < 1 {
		err = errors.New("")
		return handle("No previous unarchived version found in db", err)
	}
	key, err := generateChecksum(file, num)
	if err != nil {
		return handle("Error in generating checksum", err)
	}

	if err = moveObject(ctx, file, key); err != nil {
		return handle("Error in moving modified file to archive", err)
	}
	if err = dbArchiveFile(ctx, file, key, num); err != nil {
		return handle("Error in archiving file in db", err)
	}
	if err = deleteObject(ctx, file); err != nil {
		return handle("Error in deleting old modified file copy", err)
	}
	if err = putObject(ctx, ctx.temp+file, file); err != nil {
		return handle("Error in uploading new version of file to S3", err)
	}
	if err = dbNewVersion(ctx, file, cache); err != nil {
		errOut("Error in adding new version to db", err)
	}
	return err
}
