package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (c *Context) archiveOldVersions(tempDir string) error {
	var err error

	// Return if rsync didn't make a modified folder
	_, err = os.Stat(fmt.Sprintf("%s/%s", c.LocalPath, tempDir))
	if err != nil {
		return nil
	}

	// Make archive folder
	dest := fmt.Sprintf("%s/%s", c.LocalPath, tempDir)
	os.MkdirAll(c.LocalTop+"/archive", os.ModePerm)

	// Walk through each modified file
	if _, err := os.Stat(dest); err == nil {
		err = filepath.Walk(dest, c.archiveFile(tempDir))
	}

	return err
}

// Handle each changed file
func (c *Context) archiveFile(tempDir string) filepath.WalkFunc {
	return func(origPath string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}

		// Setup
		// Remove first part of newPath.
		newPath := origPath[len(c.LocalTop)-2:]
		// Remove the segment specifying the temporary directory.
		newPath = strings.Replace(newPath, tempDir+"/", "", 1)
		num := c.lastVersionNum(newPath, false)
		key, err := generateHash(origPath, newPath, num)
		if err != nil {
			return err
		}

		// Move to archive folder
		dest := fmt.Sprintf("%s/archive/%s", c.LocalTop[2:], key)
		err = os.Rename(origPath, dest)

		// Update the old entry with archiveKey blob
		query := fmt.Sprintf("update entries set ArchiveKey='%s' "+
			"where PathName='%s' and VersionNum=%d;", key, newPath, num)
		_, err = c.db.Exec(query)

		return err
	}
}
