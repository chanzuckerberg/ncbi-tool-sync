package main

import (
	"database/sql"
	"log"
	"path/filepath"
)

// Processes changes for new, modified, and deleted files. Modified
// and deleted files are processed by archiveOldVersions in the temp
// directory. New and modified files have new versions to be added to
// the db. Temp folder is deleted after handling.
func dbUpdateStage(ctx *Context, toSync syncResult) error {
	log.Print("Beginning db update stage.")
	var err error

	// Add new or modified files as db entries
	if err = dbNewVersions(ctx, toSync.newF); err != nil {
		return handle("Error in adding new files to db.", err)
	}
	if err = dbNewVersions(ctx, toSync.modified); err != nil {
		return handle("Error in adding new versions of modified files to db.", err)
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
	//_, mock, err := sqlmock.New()
	//if reflect.ValueOf(ctx.Db).Kind() == reflect.ValueOf(mock).Kind() {
	//	return ""
	//}
	var err error
	dir := filepath.Dir(pathName)
	file := filepath.Base(pathName)
	_, present := cache[dir]
	if !present {
		// Get listing from server
		cache[dir], err = getServerListing(dir)
		if err != nil {
			handle("Error in getting listing from FTP server.", err)
		}
	} else {
		_, present = cache[dir][file]
		if !present {
			log.Print("Error in getting FTP listing. Expected to find file in cached listing.")
			return ""
		}
	}
	return cache[dir][file]
}

// getDbModTime gets the modified time for the latest file version recorded in
// the database.
func getDbModTime(ctx *Context, file string) (string, error) {
	var res string
	err := ctx.Db.QueryRow("select DateModified from entries "+
		"where PathName=? and DateModified is not null order by VersionNum desc",
		file).Scan(&res)
	switch {
	case err == sql.ErrNoRows:
		log.Print("No entries found for: " + file)
		return "", nil
	case err != nil:
		return "", handle("Error in querying database.", err)
	}
	return res, err
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
		return handle("Error in new version query.", err)
	}
	return err
}
