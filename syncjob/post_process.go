package main

import (
	"fmt"
)

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

// Handle a list of files with new versions
func (c *Context) handleNewVersions(files []string) error {
	for _, file := range files {
		err := c.handleNewVersion(file)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handle one file with a new version on disk
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
