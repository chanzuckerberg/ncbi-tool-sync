package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jasonlvhit/gocron"
	"github.com/jlaffaye/ftp"
	"gopkg.in/fatih/set.v0"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// callSyncFlow calls the Rsync workflow. Executes a dry run first for
// processing. Then runs the sync file operations. Finally updates the db with
// changes.
func callSyncFlow(ctx *Context, repeat bool) error {
	log.Print("Start of sync flow...")
	var err error

	// Check db
	if err = ctx.Db.Ping(); err != nil {
		return handle("Failed to ping database. Aborting run.", err)
	}

	// Offset scheduling of next run so it'll only schedule after one finishes
	gocron.Clear()
	defer func() {
		if !repeat {
			return
		}
		gocron.Every(24).Hours().Do(callSyncFlow, ctx, true)
		log.Print("Next run has been scheduled...")
		<-gocron.Start()
	}()

	// Dry run analysis stage
	toSync, err := dryRunStage(ctx)
	if err != nil {
		return handle("Error in dry run stage.", err)
	}

	// File operation stage. Moving actual files around.
	fileOperationStage(ctx, toSync)

	// Db operation stage. Process changes in the db entries.
	if err = dbUpdateStage(ctx, toSync); err != nil {
		return handle("Error in processing db changes.", err)
	}

	log.Print("Finished processing changes.")
	log.Print("End of sync flow...")
	return err
}

// Executes the actual file operations on local disk and S3.
func fileOperationStage(ctx *Context, res syncResult) {
	log.Print("Beginning file operations stage.")

	log.Print("Going to handle new file operations...")
	newFilesOperations(ctx, res.newF)
	log.Print("Going to handle modified file operations...")
	modifiedFilesOperations(ctx, res.modified)
	log.Print("Going to handle deleted file operations...")
	deletedFilesOperations(ctx, res.deleted)
}

// newFilesOperations executes operations for a single file at a time for new
// files.
func newFilesOperations(ctx *Context, newF []string) {
	var err error
	for _, file := range newF {
		if err = copyFileFromRemote(ctx, file); err != nil {
			handle("Error in copying new file from remote.", err)
			continue
		}
		if err = putObject(ctx, ctx.Temp+file, file); err != nil {
			handle("Error in uploading new file to S3.", err)
		}
	}
}

// deletedFilesOperations executes operations for a single file at a time for
// deleted files.
func deletedFilesOperations(ctx *Context, newF []string) {
	var err error
	for _, file := range newF {
		if err = moveOldFile(ctx, file); err != nil {
			handle("Error in moving deleted file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			handle("Error in deleting file.", err)
		}
	}
}

// modifiedFilesOperations executes a single file at-a-time flow for modified
// files. Saves on local disk space for new files temporarily before uploading
// to S3.
func modifiedFilesOperations(ctx *Context, modified []string) {
	var err error
	for _, file := range modified {
		if err = copyFileFromRemote(ctx, file); err != nil {
			handle("Error in copying modified file from remote.", err)
			continue
		}
		if err = moveOldFile(ctx, file); err != nil {
			handle("Error in moving modified file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			handle("Error in deleting old modified file copies.", err)
		}
		if err = putObject(ctx, ctx.Temp+file, file); err != nil {
			handle("Error in uploading new version of file to S3.", err)
		}
	}
}

// Analysis stage of getting itemized output from rsync and parsing for new,
// modified, and deleted files.
func dryRunStage(ctx *Context) (syncResult, error) {
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

type fInfo struct {
	name    string
	modTime string
	size    int
}

type syncResult struct {
	newF     []string
	modified []string
	deleted  []string
}

// Runs a dry sync of main files from the source to local disk and
// parses the itemized changes.
var getChanges = getChangesSync
func getChangesSync(ctx *Context, folder syncFolder) (syncResult,
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

// Call rsync dry run to get a list of files to inspect with respect to the
// rsync filters.
func getFilteredSet(ctx *Context, folder syncFolder) (*set.Set, *set.Set,
	error) {
	toInspect := set.New()
	folderSet := set.New()
	template := "rsync -arzvn --itemize-changes --delete --no-motd " +
		"--copy-links --prune-empty-dirs"
	for _, flag := range folder.flags {
		template += " --" + flag
	}
	source := ctx.Server + folder.sourcePath + "/"
	tmp := ctx.UserHome + "/tmp"
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

func getPreviousState(ctx *Context, folder syncFolder) (map[string]fInfo,
	error) {
	// Get listing from S3 and last modtimes. Represents the previous state of
	// the directory.
	pastState := make(map[string]fInfo)
	svc := ctx.svcS3
	path := folder.sourcePath[1:] // Remove leading forward slash
	input := &s3.ListObjectsInput{
		Bucket: aws.String(ctx.Bucket),
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
			handle("Error in getting db modTime", err)
			modTime = ""
		}
		pastState[name] = fInfo{name, modTime, size}
	}
	return pastState, err
}

func getCurrentState(folderSet *set.Set, toInspect *set.Set) (map[string]fInfo,
	error) {
	res := make(map[string]fInfo)

	// Open FTP connection
	client, err := connectToServer()
	if err != nil {
		return res, handle("Error in connecting to FTP server.", err)
	}
	defer client.Quit()
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

// Archives a file by moving it from the current storage to the archive
// folder under an archiveKey name and recording it in the db.
func moveOldFile(ctx *Context, file string) error {
	// Setup
	var err error
	log.Print("Archiving old version of: " + file)
	num := lastVersionNum(ctx, file, false)
	if num < 1 {
		err = errors.New("")
		return handle("No previous unarchived version found in db", err)
	}
	key, err := generateHash(file, num)
	if err != nil {
		return handle("Error in generating checksum.", err)
	}
	if err = moveOldFileOperations(ctx, file, key); err != nil {
		handle("Error in operation to move old file to archive.", err)
	}
	if err = moveOldFileDb(ctx, key, file, num); err != nil {
		handle("Error in updating db entry for old file.", err)
	}
	return err
}

// moveOldFileOperations moves the to-be-archived file on S3 to the archive
// folder under a new file key.
func moveOldFileOperations(ctx *Context, file string, key string) error {
	// Move to archive folder
	svc := ctx.svcS3
	// Ex: bucket/remote/blast/db/README
	log.Print("Copy from: " + ctx.Bucket + file)
	log.Print("Copy-to key: " + "archive/" + key)

	// Get file size
	size, err := fileSizeOnS3(ctx, file, svc)
	if err != nil {
		return handle("Error in getting file size on S3.", err)
	}

	if size < 4500000000 {
		// Handle via S3 SDK
		err = copyOnS3(ctx, file, key, svc)
		if err != nil {
			handle("Error in copying file on S3.", err)
		}
	} else {
		log.Print("Large file handling...")
		// Handle via S3 command line tool
		template := "aws s3 mv s3://%s%s s3://%s/archive/%s"
		cmd := fmt.Sprintf(template, ctx.Bucket, file, ctx.Bucket, key)
		_, _, err = commandVerbose(cmd)
		if err != nil {
			handle("Error in moving file on S3 via CLI.", err)
		}
	}
	return err
}

// moveOldFileDb updates the old db entry with a new archive blob for
// reference.
func moveOldFileDb(ctx *Context, key string, file string, num int) error {
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Print("Db query: " + query)
	_, err := ctx.Db.Exec("update entries set ArchiveKey=? where "+
		"PathName=? and VersionNum=?;", key, file, num)
	if err != nil {
		return handle("Error in updating db entry.", err)
	}
	return err
}

// Copies one file from remote server to local disk folder with rsync.
func copyFileFromRemote(ctx *Context, file string) error {
	source := fmt.Sprintf("%s%s", ctx.Server, file)
	// Ex: $HOME/temp/blast/db
	log.Print("Local dir to make: " + ctx.Temp + filepath.Dir(file))
	err := ctx.os.MkdirAll(ctx.Temp+filepath.Dir(file), os.ModePerm)
	if err != nil {
		return handle("Couldn't make dir.", err)
	}
	// Ex: $HOME/temp/blast/db/README
	dest := fmt.Sprintf("%s%s", ctx.Temp, file)
	template := "rsync -arzv --size-only --no-motd --progress " +
		"--copy-links %s %s"
	cmd := fmt.Sprintf(template, source, dest)
	_, _, err = commandVerbose(cmd)
	if err != nil {
		return handle("Couldn't rsync file to local disk.", err)
	}
	return err
}

// Uploads one file from local disk to S3 with uploadKey.
func putObject(ctx *Context, onDisk string, uploadKey string) error {
	// Setup
	sess := session.Must(session.NewSession())
	// Ex: $HOME/temp/blast/db/README
	log.Print("File upload. Source: " + onDisk)
	local, err := ctx.os.Open(onDisk)
	if err != nil {
		return handle("Error in opening file on disk.", err)
	}
	defer local.Close()

	// Upload to S3
	uploader := s3manager.NewUploader(sess)
	output, err := uploader.Upload(&s3manager.UploadInput{
		Body:   local,
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(uploadKey),
	})
	awsOutput(fmt.Sprintf("%#v", output))
	if err != nil && !strings.Contains(err.Error(),
		"IllegalLocationConstraintException") {
		return handle(fmt.Sprintf("Error in file upload of %s to S3.", onDisk), err)
	}

	// Remove file locally after upload finished
	if err = ctx.os.Remove(onDisk); err != nil {
		return handle("Error in deleting temporary file on local disk.", err)
	}
	return err
}

// Copies a file on S3 from the CopySource to the archive folder with key.
func copyOnS3(ctx *Context, file string, key string, svc *s3.S3) error {
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(ctx.Bucket),
		CopySource: aws.String(ctx.Bucket + file),
		Key:        aws.String("archive/" + key),
	}
	output, err := svc.CopyObject(params)
	awsOutput(output.GoString())
	if err != nil {
		return handle(fmt.Sprintf("Error in copying %s on S3.", file), err)
	}
	return err
}

// Gets the size of a file on S3.
func fileSizeOnS3(ctx *Context, file string, svc *s3.S3) (int, error) {
	var result int
	input := &s3.HeadObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(file),
	}
	output, err := svc.HeadObject(input)
	awsOutput(output.GoString())
	if err != nil {
		return result, handle("Error in HeadObject request.", err)
	}
	if output.ContentLength != nil {
		result = int(*output.ContentLength)
	}
	return result, err
}

// Deletes an object on S3.
func deleteObject(ctx *Context, file string) error {
	// Setup
	svc := ctx.svcS3
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(file),
	}

	output, err := svc.DeleteObject(input)
	awsOutput(output.GoString())
	if err != nil {
		return handle("Error in deleting object.", err)
	}
	return err
}

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

func combineNames(pastState map[string]fInfo,
	newState map[string]fInfo) []string {
	combined := set.New()
	for k := range pastState {
		combined.Add(k)
	}
	for k := range newState {
		combined.Add(k)
	}
	res := []string{}
	for _, v := range combined.List() {
		res = append(res, v.(string))
	}
	sort.Strings(res)
	return res
}

func fileChangeLogic(pastState map[string]fInfo, newState map[string]fInfo,
	names []string) syncResult {
	var n, m, d []string // New, modified, deleted
	for _, f := range names {
		past, inPast := pastState[f]
		cur, inCurrent := newState[f]
		if !inPast && inCurrent {
			n = append(n, f)
		} else if inPast && !inCurrent {
			d = append(d, f)
		} else {
			if past.size != cur.size {
				m = append(m, f)
			} else {
				if strings.Contains(f, ".md5") &&
					past.modTime != cur.modTime {
					m = append(m, f)
				}
			}
		}
	}
	return syncResult{n, m, d}
}
