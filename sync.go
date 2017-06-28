package main

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"strings"
)

// Parses the Rsync itemized output for new, modified, and deleted
// files. Follows the Rsync itemized changes syntax that specify
// exactly which changes occurred or will occur.
func parseChanges(out []byte, base string) ([]string,
	[]string, []string) {
	changes := strings.Split(string(out[:]), "\n")
	changes = changes[1 : len(changes)-4] // Remove junk lines

	var newNow, modified, deleted []string

	for _, line := range changes {
		col := strings.SplitN(line, " ", 2)
		change := col[0]
		file := col[1]
		path := base + "/" + file
		last := file[len(file)-1:]
		if strings.HasPrefix(change, ">f+++++++") {
			newNow = append(newNow, path)
		} else if strings.HasPrefix(change, ">f") {
			modified = append(modified, path)
		} else if strings.HasPrefix(change, "*deleting") &&
			last != "/" {
			// Exclude folders
			deleted = append(deleted, path)
		}
	}
	return newNow, modified, deleted
}

// Calls the Rsync workflow. Executes a dry run first for processing.
// Then runs the real sync. Finally processes the changes.
func (ctx *Context) callRsyncFlow() error {
	var err error
	var cmd string

	// Construct Rsync parameters
	source := fmt.Sprintf("rsync://%s%s/", ctx.Server,
		ctx.SourcePath)
	tempDir := timeName()
	template := "rsync -abrzv %s --itemize-changes --delete " +
		"--size-only --no-motd --exclude='.*' --backup-dir='%s' %s %s"

	// Dry run
	cmd = fmt.Sprintf(template, "-n", tempDir, source, ctx.LocalPath)
	log.Println("Command: " + cmd)
	out, err := callCommand(cmd)
	if err != nil {
		log.Fatal(out, err)
		return err
	}
	newNow, modified, deleted := parseChanges(out, ctx.SourcePath)
	log.Printf("New on remote: %s", newNow)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)

	// Actual run
	log.Println("Actual sync run...")
	os.MkdirAll(ctx.LocalPath, os.ModePerm)
	cmd = fmt.Sprintf(template, "", tempDir, source, ctx.LocalPath)
	out, err = callCommand(cmd)
	if err != nil {
		log.Fatal(out, err)
		return err
	}

	// Process changes
	log.Println("Processing changes...")
	err = ctx.processChanges(newNow, modified, tempDir)
	return err
}
