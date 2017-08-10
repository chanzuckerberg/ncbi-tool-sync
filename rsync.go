package main

import (
	"fmt"
	"log"
	"path/filepath"
	"os"
	"gopkg.in/fatih/set.v0"
	"strings"
)

// copyFileFromRemote copies one file from remote server to local disk folder
// with a simple rsync call.
func copyFileFromRemote(ctx *context, file string) error {
	source := fmt.Sprintf("%s%s", ctx.server, file)
	// Ex: $HOME/temp/blast/db
	log.Print("Local dir to make: " + ctx.temp + filepath.Dir(file))
	err := ctx.os.MkdirAll(ctx.temp+filepath.Dir(file), os.ModePerm)
	if err != nil {
		return handle("Couldn't make dir.", err)
	}
	// Ex: $HOME/temp/blast/db/README
	dest := fmt.Sprintf("%s%s", ctx.temp, file)
	template := "rsync -arzv --inplace --size-only --no-motd --progress " +
		"--copy-links %s %s"
	cmd := fmt.Sprintf(template, source, dest)
	_, _, err = commandVerboseOnErr(cmd)
	if err != nil {
		return handle("Couldn't rsync file to local disk.", err)
	}
	return err
}

// listFromRsync gets a list of inspected files from rsync. Used to leverage
// rsync's built-in filtering capabilities.
func listFromRsync(out string, base string) *set.Set {
	res := set.New()
	lines := strings.Split(out, "\n")
	if len(lines) < 5 {
		return res
	}
	lines = lines[1 : len(lines)-4] // Remove junk lines
	for _, line := range lines {
		if !strings.Contains(line, " ") {
			continue
		}
		col := strings.SplitN(line, " ", 2)
		file := col[1]
		path := base + "/" + file
		res.Add(path)
	}
	return res
}

// extractSubfolders parses rsync's stdout and extracts a set of sub-folders
// that were recursively inspected. Used to make FTP listing calls more
// efficient per sub-directory.
func extractSubfolders(out string, base string) *set.Set {
	res := set.New()
	lines := strings.Split(out, "\n")
	if len(lines) < 5 {
		return res
	}
	lines = lines[2 : len(lines)-4] // Remove junk lines
	for _, line := range lines {
		col := strings.SplitN(line, " ", 2)
		file := col[1]
		dir := filepath.Dir(base + "/" + file) // Grab the folder path
		res.Add(dir)
	}
	return res
}
