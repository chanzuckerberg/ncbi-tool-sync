package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jasonlvhit/gocron"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"sort"
	"gopkg.in/fatih/set.v0"
)

// Calls the Rsync workflow. Executes a dry run first for processing.
// Then runs the real sync. Finally processes the changes.
func callSyncFlow(ctx *Context) error {
	log.Print("Start of sync flow...")
	var err error

	// Check db
	if err = ctx.Db.Ping(); err != nil {
		err = newErr("Failed to ping database. Aborting run.", err)
		log.Print(err)
		return err
	}

	// Offset scheduling of next run so it'll only schedule after one finishes
	gocron.Clear()
	defer func() {
		gocron.Every(1).Hour().Do(callSyncFlow, ctx)
		log.Print("Next run has been scheduled...")
		<-gocron.Start()
	}()

	// Dry run analysis stage
	newF, modified, deleted, err := dryRunStage(ctx)
	if err != nil {
		err = newErr("Error in dry run stage.", err)
		log.Print(err)
		return err
	}

	return err

	// File operation stage. Moving actual files around.
	fileOperationStage(ctx, newF, modified, deleted)

	// Db operation stage. Process changes in the db entries.
	err = dbUpdateStage(ctx, newF, modified)
	if err != nil {
		err = newErr("Error in processing db changes.", err)
		log.Print(err)
		return err
	}
	log.Print("Finished processing changes.")
	log.Print("End of sync flow...")
	return err
}

// Executes the actual file operations on local disk and S3.
func fileOperationStage(ctx *Context, newF []string, modified []string,
	deleted []string) {
	log.Print("Beginning file operations stage.")

	log.Print("Going to handle new file operations...")
	newFilesOperations(ctx, newF)
	log.Print("Going to handle modified file operations...")
	modifiedFilesOperations(ctx, modified)
	log.Print("Going to handle deleted file operations...")
	deletedFilesOperations(ctx, deleted)
}

// newFilesOperations executes operations for a single file at a time for new
// files.
func newFilesOperations(ctx *Context, newF []string) {
	var err error
	for _, file := range newF {
		if err = copyFileFromRemote(ctx, file); err != nil {
			err = newErr("Error in copying new file from remote.", err)
			log.Print(err)
			continue
		}
		if err = putObject(ctx, ctx.TempNew+file, file); err != nil {
			err = newErr("Error in uploading new file to S3.", err)
			log.Print(err)
		}
	}
}

// deletedFilesOperations executes operations for a single file at a time for
// deleted files.
func deletedFilesOperations(ctx *Context, newF []string) {
	var err error
	for _, file := range newF {
		if err = moveOldFile(ctx, file); err != nil {
			err = newErr("Error in moving deleted file to archive.", err)
			log.Print(err)
		}
		if err = deleteObject(ctx, file); err != nil {
			err = newErr("Error in deleting file.", err)
			log.Print(err)
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
			err = newErr("Error in copying modified file from remote.", err)
			log.Print(err)
			continue
		}
		if err = moveOldFile(ctx, file); err != nil {
			err = newErr("Error in moving modified file to archive.", err)
			log.Print(err)
		}
		if err = deleteObject(ctx, file); err != nil {
			err = newErr("Error in deleting old modified file copies.", err)
			log.Print(err)
		}
		if err = putObject(ctx, ctx.TempNew+file, file); err != nil {
			err = newErr("Error in uploading new version of file to S3.", err)
			log.Print(err)
		}
	}
}

// Analysis stage of getting itemized output from rsync and parsing for new,
// modified, and deleted files.
func dryRunStage(ctx *Context) ([]string, []string, []string, error) {
	log.Print("Beginning dry run stage.")

	// FUSE mounting steps
	//UnmountFuse(ctx)
	//MountFuse(ctx)
	//defer UnmountFuse(ctx)
	//checkMount(ctx)

	// Start go routine for checking FUSE connection continuously
	//quit := make(chan bool)
	//checkMountRepeat(ctx, quit)

	// Dry runs
	var newF, modified, deleted []string
	for _, folder := range ctx.syncFolders {
		n, m, d, err := dryRunRsync(ctx, folder)
		if err != nil {
			err = newErr("Error in running dry run.", err)
			log.Print(err)
			return newF, modified, deleted, err
		}
		newF = append(newF, n...)
		modified = append(modified, m...)
		deleted = append(deleted, d...)
	}

	//quit <- true // Terminate FUSE-checking goroutine

	// Merge list of regular files and MD5 files
	log.Print("Done with dry run...\nParsing changes...")
	log.Printf("New on remote: %s", newF)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)
	return newF, modified, deleted, nil
}

type fInfo struct {
	name    string
	modTime string
	size    int
}

// dryRunRsync runs a dry sync of main files from the source to local disk and
// parses the itemized changes.
func dryRunRsync(ctx *Context, folder syncFolder) ([]string, []string, []string,
	error) {
	// Setup
	log.Print("Running dry run...")
	var newF, modified, deleted []string

	// Get listing from origin server. Get list of files to go through with respect to filters.
	template := "rsync -arzvn --itemize-changes --delete --no-motd --copy-links --prune-empty-dirs"
	for _, flag := range folder.flags {
		template += " --" + flag
	}
	source := ctx.Server + folder.sourcePath + "/"
	tmp := ctx.LocalTop + "tmp"
	cmd := fmt.Sprintf("%s %s %s", template, source, tmp)
	if err := ctx.os.MkdirAll(tmp, os.ModePerm); err != nil {
		err = newErr("Couldn't make local tmp path.", err)
		log.Print(err)
		return newF, modified, deleted, err
	}
	stdout, _, err := commandVerboseOnErr(cmd)
	if err != nil {
		err = newErr("Error in dry run execution.", err)
		log.Print(err)
		return newF, modified, deleted, err
	}
	toInspect := listFromRsync(stdout, folder.sourcePath)
	folderSet := extractSubfolders(stdout, folder.sourcePath)
	//log.Print("folderSet:")
	//log.Println(folderSet)
	//log.Print("To inspect:")
	//log.Print(toInspect)

	// Get listing from S3 and last modtimes. Represents the previous state of
	// the directory.
	pastState := make(map[string]fInfo)
	svc := s3.New(session.Must(session.NewSession()))
	path := folder.sourcePath[1:] // Remove leading forward slash
	//fmt.Println("BUCKET: " + ctx.Bucket)
	//fmt.Println("PATH: " + path)
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(ctx.Bucket),
		Prefix:  aws.String(path),
		MaxKeys: aws.Int64(5000),
	}

	res := []*s3.Object{}
	err = svc.ListObjectsPages(input,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			res = append(res, page.Contents...)
			//fmt.Println(page)
			return true
		})
	//fmt.Println("S3 RESULTS:")
	//fmt.Println(s3Results)

	if err != nil {
		err = newErr("Error in getting listing of existing files.", err)
		log.Print(err)
		return newF, modified, deleted, err
	}
	//fmt.Println(res)
	for _, val := range res {
		//log.Println(*val.Key)
		name := "/" + *val.Key
		size := int(*val.Size)
		if !toInspect.Has(name) || size == 0 {
			//fmt.Println("SKIP: " + name)
			//fmt.Println(size)
			continue
		}
		modTime, err := getDbModTime(ctx, name)
		if err != nil {
			log.Print(err)
			modTime = ""
		}
		res := fInfo{name, modTime, size}
		pastState[name] = res
	}

	log.Print("Past state:")
	log.Print(pastState)

	// Get current listing from FTP server. Represents the new state of the
	// directory.
	newState, err := getCurrentState(folderSet, toInspect)
	log.Print("New state:")
	log.Print(newState)
	if err != nil {
		return newF, modified, deleted, handle("Error in getting current listing and metadata.", err)
	}

	// Combine the two lists of file names for processing.
	combined := make(map[string]bool)
	for k := range pastState {
		combined[k] = true
	}
	for k := range newState {
		combined[k] = true
	}
	combinedNames := []string{}
	for k := range combined {
		combinedNames = append(combinedNames, k)
	}
	sort.Strings(combinedNames)
	fmt.Printf("Combined names: %s\n", combinedNames)

	// Actual comparison steps
	for _, file := range combinedNames {
		past, inPast := pastState[file]
		cur, inCurrent := newState[file]
		if !inPast && inCurrent {
			newF = append(newF, file)
		} else if inPast && !inCurrent {
			deleted = append(deleted, file)
		} else {
			if past.size != cur.size {
				log.Printf("%s: %d vs. %d", file, past.size, cur.size)
				modified = append(modified, file)
			} else {
				if strings.Contains(file, ".md5") && past.modTime != cur.modTime {
					log.Printf("%s: %s vs. %s", file, past.modTime, cur.modTime)
					modified = append(modified, file)
				}
			}
		}
	}

	fmt.Println("NEW:")
	fmt.Println(newF)
	fmt.Println("MODIFIED:")
	fmt.Println(modified)
	fmt.Println("DELETED:")
	fmt.Println(deleted)
	fmt.Println("DONE")
	return newF, modified, deleted, err
}

func getCurrentState(folderSet *set.Set, toInspect *set.Set) (map[string]fInfo, error) {
	res := make(map[string]fInfo)

	// Open FTP connection
	client, err := connectToServer()
	if err != nil {
		err = newErr("Error in connecting to FTP server.", err)
		log.Print(err)
		return res, err
	}
	defer client.Quit()
	// Get FTP listing and metadata
	for _, dir := range folderSet.List() {
		fmt.Println("DIR: " + dir.(string))
		response, err := client.List(dir.(string))
		if err != nil {
			err = newErr("Error in FTP listing.", err)
			log.Print(err)
			return res, err
		}
		// Process results
		for _, entry := range response {
			name := dir.(string) + "/" + entry.Name
			if !toInspect.Has(name) || entry.Type != 0 {
				continue
			}
			time := entry.Time.Format(time.RFC3339)
			time = time[:len(time)-1]
			res[name] = fInfo{
				name,
				time,
				int(entry.Size),
			}
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
		log.Print("No previous unarchived version found in db.")
		return err
	}
	key, err := generateHash(file, num)
	if err != nil {
		err = newErr("Error in generating checksum.", err)
		log.Print(err)
		return err
	}
	err = moveOldFileOperations(ctx, file, key)
	if err != nil {
		err = newErr("Error in operation to move old file to archive.", err)
		log.Print(err)
	}
	err = moveOldFileDb(ctx, key, file, num)
	if err != nil {
		err = newErr("Error in updating db entry for old file.", err)
		log.Print(err)
	}
	return err
}

// moveOldFileOperations moves the to-be-archived file on S3 to the archive
// folder under a new file key.
func moveOldFileOperations(ctx *Context, file string, key string) error {
	// Move to archive folder
	svc := s3.New(session.Must(session.NewSession()))
	// Ex: bucket/remote/blast/db/README
	log.Print("Copy from: " + ctx.Bucket + file)
	log.Print("Copy-to key: " + "archive/" + key)

	// Get file size
	size, err := fileSizeOnS3(ctx, file, svc)
	if err != nil {
		err = newErr("Error in getting file size on S3.", err)
		log.Print(err)
		return err
	}

	if size < 4500000000 {
		// Handle via S3 SDK
		err = copyOnS3(ctx, file, key, svc)
		if err != nil {
			err = newErr("Error in copying file on S3.", err)
			log.Print(err)
		}
	} else {
		log.Print("Large file handling...")
		// Handle via S3 command line tool
		template := "aws s3 mv s3://%s%s s3://%s/archive/%s"
		cmd := fmt.Sprintf(template, ctx.Bucket, file, ctx.Bucket, key)
		_, _, err = commandVerbose(cmd)
		if err != nil {
			err = newErr("Error in moving file on S3 via CLI.", err)
			log.Println(err)
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
		err = newErr("Error in updating db entry.", err)
		log.Print(err)
	}
	return err
}

// Copies one file from remote server to local disk folder with rsync.
func copyFileFromRemote(ctx *Context, file string) error {
	source := fmt.Sprintf("%s%s", ctx.Server, file)
	// Ex: $HOME/tempNew/blast/db
	log.Print("Local dir to make: " + ctx.TempNew + filepath.Dir(file))
	err := ctx.os.MkdirAll(ctx.TempNew+filepath.Dir(file), os.ModePerm)
	if err != nil {
		err = newErr("Couldn't make dir.", err)
		log.Print(err)
		return err
	}
	// Ex: $HOME/tempNew/blast/db/README
	dest := fmt.Sprintf("%s%s", ctx.TempNew, file)
	template := "rsync -arzv --size-only --no-motd --progress " +
		"--copy-links %s %s"
	cmd := fmt.Sprintf(template, source, dest)
	_, _, err = commandVerbose(cmd)
	if err != nil {
		err = newErr("Couldn't rsync file to local disk.", err)
		log.Print(err)
		return err
	}
	return err
}

// Uploads one file from local disk to S3 with uploadKey.
func putObject(ctx *Context, onDisk string, uploadKey string) error {
	// Setup
	sess := session.Must(session.NewSession())
	// Ex: $HOME/tempNew/blast/db/README
	log.Print("File upload. Source: " + onDisk)
	local, err := os.Open(onDisk)
	if err != nil {
		err = newErr("Error in opening file on disk.", err)
		log.Print(err)
		return err
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
	if err != nil {
		err = newErr(fmt.Sprintf("Error in file upload of %s to S3.", onDisk), err)
		log.Print(err)
		return err
	}

	// Remove file locally after upload finished
	if err = os.Remove(onDisk); err != nil {
		err = newErr("Error in deleting temporary file on local disk.", err)
		log.Print(err)
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
		err = newErr(fmt.Sprintf("Error in copying %s on S3.", file), err)
		log.Print(err)
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
		err = newErr("Error in HeadObject request.", err)
		log.Print(err)
		return result, err
	}
	result = int(*output.ContentLength)
	return result, err
}

// Deletes an object on S3.
func deleteObject(ctx *Context, file string) error {
	// Setup
	svc := s3.New(session.Must(session.NewSession()))
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(file),
	}

	output, err := svc.DeleteObject(input)
	awsOutput(output.GoString())
	if err != nil {
		err = newErr("Error in deleting object.", err)
		log.Print(err)
	}
	return err
}

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

func listFromRsync(out string, base string) *set.Set {
	res := set.New()
	lines := strings.Split(out, "\n")
	lines = lines[1 : len(lines)-4] // Remove junk lines
	for _, line := range lines {
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
	lines = lines[2 : len(lines)-4] // Remove junk lines
	for _, line := range lines {
		col := strings.SplitN(line, " ", 2)
		file := col[1]
		dir := filepath.Dir(base + "/" + file) // Grab the folder path
		res.Add(dir)
	}
	return res
}
