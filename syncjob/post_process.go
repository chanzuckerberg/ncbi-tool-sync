package main

import (
	"fmt"
)

// Processes changes for new, modified, and deleted files. Modified
// and deleted files are processed by archiveOldVersions in the temp
// directory. New and modified files have new versions to be added to
// the db. Temp folder is deleted after handling.
func (c *Context) processChanges(new []string, modified []string,
	tempDir string) error {
	// Move replaced or deleted file versions to archive
	err := c.archiveOldVersions(tempDir)
	if err != nil {
		return err
	}

	// Add new or modified files as db entries
	err = c.handleNewVersions(new)
	if err != nil {
		return err
	}
	err = c.handleNewVersions(modified)
	if err != nil {
		return err
	}

	// Delete temp folder after handling files
	path := fmt.Sprintf("%s/%s", c.LocalPath, tempDir)
	err = c.os.RemoveAll(path)

	return err
}

// Handles a list of files with new versions.
func (c *Context) handleNewVersions(files []string) error {
	for _, file := range files {
		err := c.handleNewVersion(file)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handles one file with a new version on disk. Finds the proper
// version number for this new entry. Gets the datetime modified from
// the FTP server as a workaround for the lack of date modified times
// after syncing to S3. Adds the new entry into the db.
func (c *Context) handleNewVersion(file string) error {
	var err error

	// Set version number
	var versionNum int = 1
	prevNum := c.lastVersionNum(file, true)
	if prevNum > -1 { // Some version already exists
		versionNum = prevNum + 1
	}

	// Set datetime modified
	modTime, err := c.getServerTime(file)
	if err != nil {
		return err
	}

	// Insert into database
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("insert into entries(PathName, VersionNum, "+
		"DateModified) values('%s', %d, '%s')", file, versionNum, modTime)
	_, err = c.db.Exec(query)
	tx.Commit()

	return err
}
