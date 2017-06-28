package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Archives old versions of modified or deleted files in the backup
// folder.
func (ctx *Context) archiveOldVersions(tempDir string) error {
	var err error

	// Just return if rsync didn't make a modified folder
	_, err = ctx.os.Stat(fmt.Sprintf("%s/%s", ctx.LocalPath,
		tempDir))
	if err != nil {
		return nil
	}

	// Make archive folder
	dest := fmt.Sprintf("%s/%s", ctx.LocalPath, tempDir)
	ctx.os.MkdirAll(ctx.LocalTop+"/archive", os.ModePerm)

	// Walk through each modified file
	if _, err = ctx.os.Stat(dest); err == nil {
		err = filepath.Walk(dest, ctx.archiveFile(tempDir))
		if err != nil {
			return err
		}
	}
	return err
}

// Handles each changed file. Moves files to archive folder, records
// ArchiveKey blob from a hash in the db, and renames files.
func (ctx *Context) archiveFile(tempDir string) filepath.WalkFunc {
	return func(origPath string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}

		// Setup
		// Remove first part of newPath.
		newPath := origPath[len(ctx.LocalTop)-2:]
		// Remove the segment specifying the temporary directory.
		newPath = strings.Replace(newPath, tempDir+"/", "", 1)
		num := ctx.lastVersionNum(newPath, false)
		key, err := ctx.generateHash(origPath, newPath, num)
		if err != nil {
			return err
		}

		// Move to archive folder
		dest := fmt.Sprintf("%s/archive/%s", ctx.LocalTop[2:], key)
		err = ctx.os.Rename(origPath, dest)

		// Update the old entry with archiveKey blob
		query := fmt.Sprintf(
			"update entries set ArchiveKey='%s' where "+
				"PathName='%s' and VersionNum=%d;", key, newPath, num)
		_, err = ctx.Db.Exec(query)

		return err
	}
}
