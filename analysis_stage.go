package main

import (
	"log"
	"sort"
	"gopkg.in/fatih/set.v0"
	"github.com/jlaffaye/ftp"
	"time"
	"fmt"
	"os"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

// dryRunStage identifies changes in the files and sorts them into new,
// modified, and deleted files.
func dryRunStage(ctx *context) (syncResult, error) {
	log.Print("Beginning dry run stage.")
	r := syncResult{}

	// Dry runs
	for _, folder := range ctx.syncFolders {
		resp, err := getChanges(ctx, folder)
		if err != nil {
			return r, handle("Error in running dry run.", err)
		}
		r.newF = append(r.newF, resp.newF...)
		r.modified = append(r.modified, resp.modified...)
		r.deleted = append(r.deleted, resp.deleted...)
	}
	sort.Strings(r.newF)
	sort.Strings(r.modified)
	sort.Strings(r.deleted)

	log.Print("Done with dry run...\nParsing changes...")
	log.Printf("New on remote: %s", r.newF)
	log.Printf("Modified on remote: %s", r.modified)
	log.Printf("Deleted on remote: %s", r.deleted)
	return r, nil
}

var getChanges = getChangesSync

// getChangesSync runs a dry sync of main files from the source to local disk
// and parses the itemized changes.
func getChangesSync(ctx *context, folder syncFolder) (syncResult,
	error) {
	// Setup
	log.Print("Running dry run...")
	res := syncResult{}
	pastState, err := getPreviousState(ctx, folder)
	if err != nil {
		return res, handle("Error in getting previous directory state", err)
	}
	toInspect, folderSet, err := getFilteredSet(ctx, folder)
	if err != nil {
		return res, handle("Error in getting filtered list of files", err)
	}
	newState, err := getCurrentState(folderSet, toInspect)
	if err != nil {
		return res, handle("Error in getting current directory state.", err)
	}
	combinedNames := combineNames(pastState, newState)
	res = fileChangeLogic(pastState, newState, combinedNames)
	return res, err
}

// getFilteredSet calls an rsync dry run on an empty folder to get a set of
// files to inspect with respect to the rsync filters. Used to support more
// robust filtering from rsync's built-in functionality.
func getFilteredSet(ctx *context, folder syncFolder) (*set.Set, *set.Set,
	error) {
	toInspect := set.New()
	folderSet := set.New()
	template := "rsync -arzvn --inplace --itemize-changes --delete --no-motd " +
		"--copy-links --prune-empty-dirs"
	for _, flag := range folder.flags {
		template += " --" + flag
	}
	source := ctx.server + folder.sourcePath + "/"
	tmp := ctx.local + "/synctmp"
	cmd := fmt.Sprintf("%s %s %s", template, source, tmp)
	if err := ctx.os.MkdirAll(tmp, os.ModePerm); err != nil {
		return toInspect, folderSet, handle("Couldn't make local tmp path.", err)
	}
	stdout, _, err := commandVerboseOnErr(cmd)
	if err != nil {
		return toInspect, folderSet, handle("Error in dry run execution.", err)
	}
	toInspect = listFromRsync(stdout, folder.sourcePath)
	folderSet = extractSubfolders(stdout, folder.sourcePath)
	return toInspect, folderSet, err
}

// getPreviousState gets a representation of the previous saved state of the
// folder to sync. Gets the file listing from S3 and then the modified times
// from the database. Returns a map of the file path names to fInfo metadata
// structs.
func getPreviousState(ctx *context, folder syncFolder) (map[string]fInfo,
	error) {
	// Get listing from S3 and last modtimes. Represents the previous state of
	// the directory.
	pastState := make(map[string]fInfo)
	svc := ctx.svcS3
	path := folder.sourcePath[1:] // Remove leading forward slash
	input := &s3.ListObjectsInput{
		Bucket: aws.String(ctx.bucket),
		Prefix: aws.String(path),
	}
	response := []*s3.Object{}
	err := svc.ListObjectsPages(input,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			response = append(response, page.Contents...)
			return true
		})
	if err != nil {
		return pastState, handle("Error in getting listing of existing files.", err)
	}

	var modTime string
	for _, val := range response {
		name := "/" + *val.Key
		size := int(*val.Size)
		if size == 0 {
			continue
		}
		modTime, err = getDbModTime(ctx, name)
		if err != nil {
			errOut("Error in getting db modTime", err)
			modTime = ""
		}
		pastState[name] = fInfo{name, modTime, size}
	}
	return pastState, err
}

// getCurrentState gets a representation of the current state of the folder to
// sync on the remote server. Gets the file listing and metadata via FTP.
// Returns a map of the file path names to fInfo metadata structs.
func getCurrentState(folderSet *set.Set, toInspect *set.Set) (map[string]fInfo,
	error) {
	res := make(map[string]fInfo)

	// Open FTP connection
	client, err := connectToServer()
	if err != nil {
		return res, handle("Error in connecting to FTP server.", err)
	}
	defer func() {
		if err = client.Quit(); err != nil {
			errOut("Error in quitting client", err)
		}
	}()
	// Get FTP listing and metadata
	var resp []*ftp.Entry
	for _, dir := range folderSet.List() {
		resp, err = clientList(client, dir.(string))
		if err != nil {
			return res, handle("Error in FTP listing.", err)
		}
		// Process results
		for _, entry := range resp {
			name := dir.(string) + "/" + entry.Name
			if !toInspect.Has(name) || entry.Type != 0 {
				continue
			}
			t := entry.Time.Format(time.RFC3339)
			t = t[:len(t)-1]
			res[name] = fInfo{name, t, int(entry.Size)}
		}
	}
	return res, err
}
