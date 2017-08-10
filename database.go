package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

var setupDatabase = setupDatabaseWithCtx
var lastVersionNum = lastVersionNumDb

// setupDatabaseWithCtx sets up the database from environment variables and
// checks connection conditions.
func setupDatabaseWithCtx(ctx *context) (string, error) {
	var err error
	// Setup RDS db from env variables
	rdsHostname := os.Getenv("RDS_HOSTNAME")
	rdsPort := os.Getenv("RDS_PORT")
	rdsDbName := os.Getenv("RDS_DB_NAME")
	rdsUsername := os.Getenv("RDS_USERNAME")
	rdsPassword := os.Getenv("RDS_PASSWORD")
	sourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		rdsUsername, rdsPassword, rdsHostname, rdsPort, rdsDbName)
	log.Print("DB connection string: " + sourceName)

	if ctx.db, err = sql.Open("mysql", sourceName); err != nil {
		return sourceName, handle("Failed to set up database opener", err)
	}
	if err = ctx.db.Ping(); err != nil {
		return sourceName, handle("Failed to ping database", err)
	}
	CreateTable(ctx)
	log.Print("Successfully checked database.")
	return sourceName, err
}

// CreateTable creates the table and schema in the db if not present.
func CreateTable(ctx *context) {
	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(500) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified DATETIME, " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	if _, err := ctx.db.Exec(query); err != nil {
		log.Print(err)
		log.Fatal("Failed to find or create table.")
	}
}

// moveOldFileDb updates the old db entry with a new archive blob for
// reference.
func moveOldFileDb(ctx *context, key string, file string, num int) error {
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Print("db query: " + query)
	_, err := ctx.db.Exec("update entries set ArchiveKey=? where "+
		"PathName=? and VersionNum=?;", key, file, num)
	if err != nil {
		return handle("Error in updating db entry.", err)
	}
	return err
}

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

// getDbModTime gets the modified time for the latest file version recorded in
// the database.
func getDbModTime(ctx *context, file string) (string, error) {
	var res string
	err := ctx.db.QueryRow("select DateModified from entries "+
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
		_, err = ctx.db.Exec("insert into entries(PathName, "+
			"VersionNum, DateModified) values(?, ?, ?)", pathName,
			versionNum, modTime)
	} else {
		_, err = ctx.db.Exec("insert into entries(PathName, "+
			"VersionNum) values(?, ?)", pathName, versionNum)
	}
	if err != nil {
		return handle("Error in new version query.", err)
	}
	return err
}

// lastVersionNumDb finds the latest version number of the file in the db.
func lastVersionNumDb(ctx *context, file string, inclArchive bool) int {
	num := -1
	var err error
	var rows *sql.Rows

	// Query
	if inclArchive {
		rows, err = ctx.db.Query("select VersionNum from entries "+
			"where PathName=? order by VersionNum desc", file)
	} else {
		// Specify not to include archived entries
		rows, err = ctx.db.Query("select VersionNum from entries "+
			"where PathName=? and ArchiveKey is null order by VersionNum desc",
			file)
	}
	if err != nil {
		errOut("Error in getting VersionNum.", err)
		return num
	}
	defer func() {
		if err = rows.Close(); err != nil {
			errOut("Error in closing rows", err)
		}
	}()

	if rows.Next() {
		err = rows.Scan(&num)
		if err != nil {
			errOut("Error scanning row.", err)
		}
	}
	return num
}
