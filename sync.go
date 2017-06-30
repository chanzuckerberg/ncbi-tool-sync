package main

import (
	"fmt"
	"log"
	"strings"
	"os"
)

// Parses the Rsync itemized output for new, modified, and deleted
// files. Follows the Rsync itemized changes syntax that specify
// exactly which changes occurred or will occur.
func parseChanges(out string, base string) ([]string,
	[]string, []string) {
	changes := strings.Split(out, "\n")
	changes = changes[1 : len(changes)-4] // Remove junk lines

	var newF, modified, deleted []string

	for _, line := range changes {
		col := strings.SplitN(line, " ", 2)
		change := col[0]
		file := col[1]
		path := base + "/" + file
		last := file[len(file)-1:]
		if strings.HasPrefix(change, ">f+++++++") {
			newF = append(newF, path)
		} else if strings.HasPrefix(change, ">f") {
			modified = append(modified, path)
		} else if strings.HasPrefix(change, "*deleting") &&
			last != "/" {
			// Exclude folders
			deleted = append(deleted, path)
		}
	}
	return newF, modified, deleted
}

// Calls the Rsync workflow. Executes a dry run first for processing.
// Then runs the real sync. Finally processes the changes.
func (ctx *Context) callRsyncFlow() error {
	var err error
	var cmd string

	// Construct Rsync parameters
	source := fmt.Sprintf("%s%s/", ctx.Server, ctx.SourcePath)
	tempDir := setTempDir()
	template := "rsync -abrzv %s --itemize-changes --delete " +
		"--size-only --no-motd --exclude '.*' --exclude 'cloud/*' " +
		"--exclude 'nr.gz' --exclude 'nt.gz' --exclude " +
		"'other_genomic.gz' --exclude 'refseq_genomic*' " +
		"--copy-links --prune-empty-dirs --backup-dir='%s' %s %s"

	// Dry run
	err = os.MkdirAll(ctx.LocalPath, os.ModePerm)
	if err != nil {
		log.Println("Couldn't make local path: "+err.Error())
	}
	cmd = fmt.Sprintf(template, "-n", tempDir, source, ctx.LocalPath)
	log.Println("Beginning Rsync execution... dry run...")
	stdout, _, err := commandVerbose(cmd)
	if err != nil {
		log.Println(err)
		log.Fatal("Error in running dry run.")
	}

	log.Println("Done with dry run...\nParsing changes...")
	newF, modified, deleted := parseChanges(stdout, ctx.SourcePath)
	log.Printf("New on remote: %s", newF)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)

	// Actual run
	log.Println("Actual sync run...")
	template = "rsync -abrzv %s --delete " +
		"--size-only --no-motd --progress --stats -h --exclude '.*' --exclude 'cloud/*' --exclude 'nr.gz' --exclude 'nt.gz' --exclude 'other_genomic.gz' --exclude 'refseq_genomic*' --copy-links --prune-empty-dirs --backup-dir='%s' %s %s"
	cmd = fmt.Sprintf(template, "", tempDir, source, ctx.LocalPath)
	commandStreaming(cmd)

	// Process changes
	log.Println("Done with real run...\nProcessing changes...")
	err = ctx.processChanges(newF, modified, tempDir)
	log.Println("Finished processing changes.")
	return err
}
