package main

import (
	"log"
	"os"
	"path/filepath"
	"reflect"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// Processes changes for new, modified, and deleted files. Modified
// and deleted files are processed by archiveOldVersions in the temp
// directory. New and modified files have new versions to be added to
// the db. Temp folder is deleted after handling.
func dbUpdateStage(ctx *Context, newF []string, modified []string) error {
	log.Print("Beginning db update stage.")

	// Add new or modified files as db entries
	err := dbNewVersions(ctx, newF)
	if err != nil {
		err = newErr("Error in adding new files to db.", err)
		log.Print(err)
		return err
	}
	err = dbNewVersions(ctx, modified)
	if err != nil {
		err = newErr("Error in adding new versions of modified files to db.", err)
		log.Print(err)
		return err
	}
	return err
}

// Handles a list of files with new versions.
func dbNewVersions(ctx *Context, files []string) error {
	// Cache is a map of directory names to a map of file names to mod
	// times.
	cache := make(map[string]map[string]string)

	for _, file := range files {
		dbNewVersion(ctx, file, cache)
	}
	return nil
}

// Gets the date modified times from the FTP server utilizing a
// directory listing cache.
func getModTime(ctx *Context, pathName string,
	cache map[string]map[string]string) string {
	// Workaround for avoiding FTP call if in test mode. Can be replaced by
	// mocking the FTP interface.
	_, mock, err := sqlmock.New()
	if reflect.ValueOf(ctx.Db).Kind() == reflect.ValueOf(mock).Kind() {
		return ""
	}

	dir := filepath.Dir(pathName)
	file := filepath.Base(pathName)
	_, present := cache[dir]
	if !present {
		// Get listing from server
		cache[dir], err = getServerListing(dir)
		if err != nil {
			err = newErr("Error in getting listing from FTP server.", err)
			log.Print(err)
		}
	}
	return cache[dir][file]
}

// Handles one file with a new version on disk. Finds the proper
// version number for this new entry. Gets the datetime modified from
// the FTP server as a workaround for the lack of date modified times
// after syncing to S3. Adds the new entry into the db.
func dbNewVersion(ctx *Context, pathName string,
	cache map[string]map[string]string) error {
	var err error
	log.Print("Handling new version of: " + pathName)

	// Set version number
	versionNum := 1
	prevNum := lastVersionNum(ctx, pathName, true)
	if prevNum > -1 {
		// Some version already exists
		versionNum = prevNum + 1
	}

	// Set datetime modified using directory listing cache
	modTime := getModTime(ctx, pathName, cache)

	// Insert into database
	if modTime != "" {
		_, err = ctx.Db.Exec("insert into entries(PathName, "+
			"VersionNum, DateModified) values(?, ?, ?)", pathName,
			versionNum, modTime)
	} else {
		_, err = ctx.Db.Exec("insert into entries(PathName, "+
			"VersionNum) values(?, ?)", pathName, versionNum)
	}
	if err != nil {
		err = newErr("Error in new version query.", err)
		log.Print(err)
		return err
	}
	return err
}

// Ingests all the files in the working directory as new files. Used
// for rebuilding the database or setup after a manual sync. Assumes
// that the db is already connected.
func ingestCurrentFiles(ctx *Context, dest string) error {
	_, err := ctx.os.Stat(dest)
	if err != nil {
		log.Print("No files found in: " + dest)
		return err
	}

	// Construct a list of all the file path names recursively
	log.Print("Starting to ingest all existing files into db...")
	fileList := []string{}
	err = filepath.Walk(dest,
		func(path string, f os.FileInfo, err error) error {
			info, err := os.Stat(path)
			if info.IsDir() {
				return nil
			}
			if err != nil {
				err = newErr("Error in walking file: "+path+".", err)
				log.Print(err)
				return err
			}

			snippet := path[len(ctx.LocalTop):]
			fileList = append(fileList, snippet)
			return nil
		})
	if err != nil {
		err = newErr("Error in walking files.", err)
		log.Print(err)
		return err
	}

	fileList = fileList[1:] // Skip the folder itself
	err = dbNewVersions(ctx, fileList)
	if err != nil {
		err = newErr("Error in handling new versions.", err)
		log.Print(err)
		return err
	}
	log.Print("Done ingesting existing files into db.")
	return nil
}
