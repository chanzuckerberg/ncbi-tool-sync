package main

import (
	"fmt"
	"os"
	"path/filepath"
	"log"
)

// Processes changes for new, modified, and deleted files. Modified
// and deleted files are processed by archiveOldVersions in the temp
// directory. New and modified files have new versions to be added to
// the db. Temp folder is deleted after handling.
func (ctx *Context) processChanges(new []string, modified []string,
	tempDir string) error {
	// Move replaced or deleted file versions to archive
	err := ctx.archiveOldVersions(tempDir)
	if err != nil {
		return err
	}

	// Add new or modified files as db entries
	err = ctx.handleNewVersions(new)
	if err != nil {
		return err
	}
	err = ctx.handleNewVersions(modified)
	if err != nil {
		return err
	}

	// Delete temp folder after handling files
	path := fmt.Sprintf("%s/%s", ctx.LocalPath, tempDir)
	err = ctx.os.RemoveAll(path)

	return err
}

// Handles a list of files with new versions.
func (ctx *Context) handleNewVersions(files []string) error {
	// Cache is a map of directory names to a map of file names to mod
	// times.
	cache := make(map[string]map[string]string)

	for _, file := range files {
		err := ctx.handleNewVersion(file, cache)
		if err != nil {
			log.Println(err) // Only log the error and continue
		}
	}
	return nil
}

// Gets the date modified times from the FTP server utilizing a
// directory listing cache.
func (ctx *Context) getModTime(pathName string,
	cache map[string]map[string]string) string {
	dir := filepath.Dir(pathName)
	file := filepath.Base(pathName)
	_, present := cache[dir]
	var err error
	if !present {
		// Get listing from server
		cache[dir], err = ctx.getServerListing(dir)
		if err != nil {
			log.Println(err)
		}
	}
	return cache[dir][file]
}

// Handles one file with a new version on disk. Finds the proper
// version number for this new entry. Gets the datetime modified from
// the FTP server as a workaround for the lack of date modified times
// after syncing to S3. Adds the new entry into the db.
func (ctx *Context) handleNewVersion(pathName string,
	cache map[string]map[string]string) error {
	var err error

	// Set version number
	var versionNum int = 1
	prevNum := ctx.lastVersionNum(pathName, true)
	if prevNum > -1 {
		// Some version already exists
		versionNum = prevNum + 1
	}

	// Set datetime modified using directory listing cache
	modTime := ctx.getModTime(pathName, cache)

	// Insert into database
	tx, err := ctx.Db.Begin()
	if err != nil {
		return err
	}
	query := ""
	if modTime != "" {
		query = fmt.Sprintf("insert into entries(PathName, " +
			"VersionNum, DateModified) values('%s', %d, '%s')", pathName,
			versionNum, modTime)
	} else {
		query = fmt.Sprintf("insert into entries(PathName, " +
			"VersionNum) values('%s', %d)", pathName, versionNum)
	}
	_, err = ctx.Db.Exec(query)
	tx.Commit()

	return err
}

// Ingests all the files in the working directory as new files. Used
// for rebuilding the database or setup after a manual sync. Assumes
// that the db is already connected.
func (ctx *Context) ingestCurrentFiles() error {
	dest := fmt.Sprintf("%s", ctx.LocalPath)
	_, err := ctx.os.Stat(dest)
	if err != nil {
		return err
	}

	// Construct a list of all the file path names recursively
	log.Println("Starting to ingest all existing files into db...")
	fileList := []string{}
	err = filepath.Walk(dest,
		func(path string, f os.FileInfo, err error) error {
			snippet := path[len(ctx.LocalTop)-2:]
			fileList = append(fileList, snippet)
			return nil
	})

	fileList = fileList[1:] // Skip the folder itself
	ctx.handleNewVersions(fileList)
	log.Println("Done ingesting existing files into db.")
	return nil
}