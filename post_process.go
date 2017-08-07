package main

import (
	"database/sql"
	"errors"
	"log"
	"path/filepath"
)

// dbUpdateStage adds new file versions to the database.
func dbUpdateStage(ctx *context, toSync syncResult) error {
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

// dbNewVersions handles a list of files with new versions.
func dbNewVersions(ctx *context, files []string) error {
	// Cache is a map of directory names to a map of file names to mod
	// times.
	cache := make(map[string]map[string]string)
	for _, file := range files {
		if err := dbNewVersion(ctx, file, cache); err != nil {
			errOut("Error in adding new version to db", err)
		}
	}
	return nil
}

var getModTime = getModTimeFTP

// getModTimeFTP gets the date modified times from the FTP server utilizing a
// directory listing cache.
func getModTimeFTP(path string, cache map[string]map[string]string) string {
	var err error
	dir := filepath.Dir(path)
	file := filepath.Base(path)
	_, present := cache[dir]
	if !present {
		// Get listing from server
		cache[dir], err = getServerListing(dir)
		if err != nil {
			errOut("Error in getting listing from FTP server.", err)
		}
	} else {
		_, present = cache[dir][file]
		if !present {
			err = errors.New("")
			errOut("Error in getting FTP listing. Expected to find file in cached "+
				"listing.", err)
			return ""
		}
	}
	return cache[dir][file]
}

// getDbModTime gets the modified time for the latest file version recorded in
// the database.
func getDbModTime(ctx *context, file string) (string, error) {
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

// dbNewVersion handles one file with a new version on disk. Sets the version
// number for the new entry. Gets the datetime modified from the FTP server as
// a workaround for the lack of original date modified times after syncing to
// S3. Adds the new entry into the db.
func dbNewVersion(ctx *context, pathName string,
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
	modTime := getModTime(pathName, cache)

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
