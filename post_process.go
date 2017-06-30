package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Processes changes for new, modified, and deleted files. Modified
// and deleted files are processed by archiveOldVersions in the temp
// directory. New and modified files have new versions to be added to
// the db. Temp folder is deleted after handling.
func (ctx *Context) processChanges(newF []string, modified []string,
	tempDir string) error {
	// Move replaced or deleted file versions to archive
	err := ctx.archiveOldVersions(tempDir)
	if err != nil {
		return err
	}

	// Add new or modified files as db entries
	err = ctx.handleNewVersions(newF)
	if err != nil {
		return err
	}
	err = ctx.handleNewVersions(modified)
	if err != nil {
		return err
	}

	// Delete temp folder after handling files
	err = ctx.os.RemoveAll(tempDir)

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
	log.Println("Handling new version of: "+pathName)

	// Set version number
	versionNum := 1
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
		query = fmt.Sprintf("insert into entries(PathName, "+
			"VersionNum, DateModified) values('%s', %d, '%s')", pathName,
			versionNum, modTime)
	} else {
		query = fmt.Sprintf("insert into entries(PathName, "+
			"VersionNum) values('%s', %d)", pathName, versionNum)
	}
	_, err = ctx.Db.Exec(query)
	err = tx.Commit()
	if err != nil {
		log.Println("Error committing transaction: "+err.Error())
	}

	return err
}

// Ingests all the files in the working directory as new files. Used
// for rebuilding the database or setup after a manual sync. Assumes
// that the db is already connected.
func (ctx *Context) ingestCurrentFiles() error {
	dest := ctx.LocalPath
	_, err := ctx.os.Stat(dest)
	if err != nil {
		log.Println("No files found in: " + dest)
		return err
	}

	// Construct a list of all the file path names recursively
	log.Println("Starting to ingest all existing files into db...")
	fileList := []string{}
	err = filepath.Walk(dest,
		func(path string, f os.FileInfo, err error) error {
			info, err := os.Stat(path)
			if info.IsDir() {
				return nil
			}
			if err != nil {
				log.Println("Error in walking file: " + path)
				log.Println(err.Error())
			}

			snippet := path[len(ctx.LocalTop):]
			fileList = append(fileList, snippet)
			return nil
		})
	if err != nil {
		log.Println("Error in walking files: " + err.Error())
	}

	fileList = fileList[1:] // Skip the folder itself
	err = ctx.handleNewVersions(fileList)
	if err != nil {
		log.Println("Error in handling new versions: " + err.Error())
	}
	log.Println("Done ingesting existing files into db.")
	return nil
}
