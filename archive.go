package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"log"
	"os/exec"
)

// Archives old versions of modified or deleted files in the backup
// folder.
func (ctx *Context) archiveOldVersions(dest string) error {
	var err error

	// Just return if rsync didn't make a modified folder
	_, err = ctx.os.Stat(dest)
	if err != nil {
		return nil
	}

	// Make archive folder
	err = ctx.os.MkdirAll(ctx.Archive, os.ModePerm)
	if err != nil {
		log.Fatal("Couldn't make archive folder: "+err.Error())
	}

	// Walk through each modified file
	if _, err = ctx.os.Stat(dest); err == nil {
		err = filepath.Walk(dest, ctx.archiveFile(dest))
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
		first := string(filepath.Base(origPath)[0])

		// Skip conditions
		if f.IsDir() || first == "." {
			return nil
		}

		// Setup
		// Remove the local part of the path
		newPath := origPath[len(ctx.UserHome):]
		// Remove the segment with the temporary directory
		newPath = strings.Replace(newPath, "backupFolder/", "", 1)
		newPath = ctx.SourcePath + newPath
		log.Println("Archiving old version of: "+newPath)
		num := ctx.lastVersionNum(newPath, false)
		key, err := ctx.generateHash(origPath, newPath, num)
		if err != nil {
			return err
		}

		// Move to archive folder
		dest := ctx.Archive + "/" + key
		toRun := fmt.Sprintf("mv %s %s", origPath, dest)
		out, err := exec.Command("sh", "-cx", toRun).CombinedOutput()
		log.Printf("%s", out)
		if err != nil {
			log.Println("Error moving changed file to archive folder.")
			log.Println(err.Error())
		}

		// Update the old entry with archiveKey blob
		query := fmt.Sprintf(
			"update entries set ArchiveKey='%s' where "+
				"PathName='%s' and VersionNum=%d;", key, newPath, num)
		log.Println("Db query: " + query)
		_, err = ctx.Db.Exec(query)
		if err != nil {
			log.Println("Error in updating entry: " + err.Error())
		}

		return err
	}
}
