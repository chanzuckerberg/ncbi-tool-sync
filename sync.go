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

var callSyncFlow = callSyncFlowRepeat

// callSyncFlowRepeat calls the Rsync workflow. Executes a dry run first for
// identifying changes. Then runs the actual file sync operations. Finally
// updates the db with changes. Gocron schedules repeating runs.
func callSyncFlowRepeat(ctx *context, repeat bool) error {
	log.Print("Start of sync flow...")
	var err error

	// Check db
	if err = ctx.db.Ping(); err != nil {
		return handle("Failed to ping database. Aborting run.", err)
	}

	// Offset scheduling of next run so it'll only schedule after this one
	// finishes.
	gocron.Clear()
	defer func() {
		if !repeat {
			return
		}
		gocron.Every(24).Hours().Do(callSyncFlowRepeat, ctx, true)
		log.Print("Next run has been scheduled...")
		<-gocron.Start()
	}()

	// Dry run analysis stage for identifying file changes.
	toSync, err := dryRunStage(ctx)
	if err != nil {
		return handle("Error in dry run stage.", err)
	}

	// File operation stage. Moving actual files around.
	fileOperationStage(ctx, toSync)

	// db operation stage. Process changes in the db entries.
	if err = dbUpdateStage(ctx, toSync); err != nil {
		return handle("Error in processing db changes.", err)
	}

	log.Print("Finished processing changes.")
	log.Print("End of sync flow...")
	return err
}

// fileOperationStage executes the actual file operations on local disk and S3.
func fileOperationStage(ctx *context, res syncResult) {
	log.Print("Beginning file operations stage.")

	log.Print("Going to handle new file operations...")
	newFilesOperations(ctx, res.newF)
	log.Print("Going to handle modified file operations...")
	modifiedFilesOperations(ctx, res.modified)
	log.Print("Going to handle deleted file operations...")
	deletedFilesOperations(ctx, res.deleted)
}

// newFilesOperations executes operations for new files. Copies files from
// remote server and uploads to S3.
func newFilesOperations(ctx *context, newF []string) {
	var err error
	for _, file := range newF {
		if err = copyFileFromRemote(ctx, file); err != nil {
			errOut("Error in copying new file from remote.", err)
			continue
		}
		if err = putObject(ctx, ctx.temp+file, file); err != nil {
			errOut("Error in uploading new file to S3.", err)
		}
	}
}

// deletedFilesOperations executes operations for deleted files. Moves old
// copies to archive and deletes the current copy.
func deletedFilesOperations(ctx *context, newF []string) {
	var err error
	for _, file := range newF {
		if err = moveOldFile(ctx, file); err != nil {
			errOut("Error in moving deleted file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			errOut("Error in deleting file.", err)
		}
	}
}

// modifiedFilesOperations executes a single file at-a-time flow for modified
// files. Copies files from remote, moves old files to archive, deletes
// current copy, and uploads new copy.
func modifiedFilesOperations(ctx *context, modified []string) {
	var err error
	for _, file := range modified {
		if err = copyFileFromRemote(ctx, file); err != nil {
			errOut("Error in copying modified file from remote.", err)
			continue
		}
		if err = moveOldFile(ctx, file); err != nil {
			errOut("Error in moving modified file to archive.", err)
		}
		if err = deleteObject(ctx, file); err != nil {
			errOut("Error in deleting old modified file copies.", err)
		}
		if err = putObject(ctx, ctx.temp+file, file); err != nil {
			errOut("Error in uploading new version of file to S3.", err)
		}
	}
}

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

// An fInfo represents file path name, modified time, and size in bytes.
type fInfo struct {
	name    string
	modTime string
	size    int
}

// A syncResult represents lists of new, modified, and deleted files.
type syncResult struct {
	newF     []string
	modified []string
	deleted  []string
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

// moveOldFile archives a file by moving it from the current directory to the
// archive folder under an archiveKey name and recording it in the db.
func moveOldFile(ctx *context, file string) error {
	// Setup
	var err error
	log.Print("Archiving old version of: " + file)
	num := lastVersionNum(ctx, file, false)
	if num < 1 {
		err = errors.New("")
		return handle("No previous unarchived version found in db", err)
	}
	key, err := generateChecksum(file, num)
	if err != nil {
		return handle("Error in generating checksum.", err)
	}
	if err = moveOldFileOperations(ctx, file, key); err != nil {
		errOut("Error in operation to move old file to archive.", err)
	}
	if err = moveOldFileDb(ctx, key, file, num); err != nil {
		errOut("Error in updating db entry for old file.", err)
	}
	return err
}

// moveOldFileOperations moves the to-be-archived file on S3 to the archive
// folder under a new file key.
func moveOldFileOperations(ctx *context, file string, key string) error {
	// Move to archive folder
	svc := ctx.svcS3
	// Ex: bucket/remote/blast/db/README
	log.Print("Copy from: " + ctx.bucket + file)
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
			errOut("Error in copying file on S3.", err)
		}
	} else {
		log.Print("Large file handling...")
		// Handle via S3 command line tool
		template := "aws s3 mv s3://%s%s s3://%s/archive/%s"
		cmd := fmt.Sprintf(template, ctx.bucket, file, ctx.bucket, key)
		_, _, err = commandVerbose(cmd)
		if err != nil {
			errOut("Error in moving file on S3 via CLI.", err)
		}
	}
	return err
}

// moveOldFileDb updates the old db entry with a new archive blob for
// reference.
func moveOldFileDb(ctx *context, key string, file string, num int) error {
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Print("db query: " + query)
	_, err := ctx.db.Exec("update entries set ArchiveKey=? where "+
		"PathName=? and VersionNum=?;", key, file, num)
	if err != nil {
		return handle("Error in updating db entry.", err)
	}
	return err
}

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

// putObject uploads one file from local disk to S3 with an uploadKey.
func putObject(ctx *context, onDisk string, uploadKey string) error {
	// Setup
	sess := session.Must(session.NewSession())
	// Ex: $HOME/temp/blast/db/README
	log.Print("File upload. Source: " + onDisk)
	local, err := ctx.os.Open(onDisk)
	if err != nil {
		return handle("Error in opening file on disk.", err)
	}
	defer func() {
		if err = local.Close(); err != nil {
			errOut("Error in closing local file", err)
		}
	}()

	// Upload to S3
	uploader := s3manager.NewUploader(sess)
	output, err := uploader.Upload(&s3manager.UploadInput{
		Body:   local,
		Bucket: aws.String(ctx.bucket),
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

// copyOnS3 copies a file on S3 from its current location to the archive folder
// under a new key.
func copyOnS3(ctx *context, file string, key string, svc *s3.S3) error {
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(ctx.bucket),
		CopySource: aws.String(ctx.bucket + file),
		Key:        aws.String("archive/" + key),
	}
	output, err := svc.CopyObject(params)
	awsOutput(output.GoString())
	if err != nil {
		return handle(fmt.Sprintf("Error in copying %s on S3.", file), err)
	}
	return err
}

var fileSizeOnS3 = fileSizeOnS3Svc

// fileSizeOnS3Svc gets the size of a file on S3.
func fileSizeOnS3Svc(ctx *context, file string, svc *s3.S3) (int, error) {
	var result int
	input := &s3.HeadObjectInput{
		Bucket: aws.String(ctx.bucket),
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

// deleteObject deletes an object on S3.
func deleteObject(ctx *context, file string) error {
	// Setup
	svc := ctx.svcS3
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.bucket),
		Key:    aws.String(file),
	}

	output, err := svc.DeleteObject(input)
	awsOutput(output.GoString())
	if err != nil {
		return handle("Error in deleting object.", err)
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

// combineNames combines the file names from pastState and newState
// representations. Used to return an overall list of files to compare as
// new, modified, or deleted.
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

// fileChangeLogic goes through a list of file names and decides if they are
// new on remote, modified, deleted, or unchanged. Uses the pastState and
// newState representations. Returns changes in a syncResult.
func fileChangeLogic(pastState map[string]fInfo, newState map[string]fInfo,
	names []string) syncResult {
	var n, m, d []string // New, modified, deleted
	for _, f := range names {
		past, inPast := pastState[f]
		cur, inCurrent := newState[f]
		if !inPast && inCurrent {
			// If not inPast and inCurrent, file is new on remote.
			n = append(n, f)
		} else if inPast && !inCurrent {
			// If inPast and not inCurrent, file is deleted on remote.
			d = append(d, f)
		} else {
			// If file size has changed, it was modified.
			if past.size != cur.size {
				m = append(m, f)
			} else {
				// Count md5 files as modified if their modTime has changed.
				if strings.Contains(f, ".md5") &&
					past.modTime != cur.modTime {
					m = append(m, f)
				}
			}
		}
	}
	return syncResult{n, m, d}
}
