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
func (ctx *Context) callSyncFlow() error {
	var err error
	log.Print("Start of sync flow...")

	// Offset scheduling of next run so it'll only schedule after one finishes
	gocron.Clear()
	defer func() {
		gocron.Every(1).Hour().Do(ctx.callSyncFlow)
		log.Print("Next run has been scheduled...")
		<-gocron.Start()
	}()

	// Dry run analysis stage
	newF, modified, deleted, err := ctx.dryRunStage()
	if err != nil {
		err = newErr("Error in dry run stage.", err)
		log.Print(err)
		return err
	}

	// File operation stage. Moving actual files around.
	ctx.fileOperationStage(newF, modified, deleted)

	// Db operation stage. Process changes in the db entries.
	err = ctx.dbUpdateStage(newF, modified)
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
func (ctx *Context) fileOperationStage(newF []string,
	modified []string, deleted []string) {
	log.Print("Beginning file operations stage.")

	// Copy from NCBI remote to local temp folder
	log.Print("Going to copy new and modified files from remote...")
	ctx.copyFilesFromRemote(newF)
	ctx.copyFilesFromRemote(modified)

	// Move files around on S3 to be replaced. Moves the to-be-replaced
	// files to the archive folder on S3 and renames them.
	log.Print("Going to move around modified and deleted files on " +
		"remote...")
	ctx.moveOldFiles(modified)
	ctx.moveOldFiles(deleted)

	// Delete files to-be-deleted on S3
	log.Print("Going to delete files on remote...")
	ctx.deleteObjects(modified)
	ctx.deleteObjects(deleted)

	// Upload new files saved locally to S3
	log.Print("Going to upload local temp files to remote...")
	ctx.putObjects(newF)
	ctx.putObjects(modified)
}

// Analysis stage of getting itemized output from rsync and parsing for new,
// modified, and deleted files.
func (ctx *Context) dryRunStage() ([]string, []string, []string, error) {
	var err error
	var newF, modified, deleted []string
	log.Print("Beginning dry run stage.")

	// FUSE mounting steps
	ctx.UnmountFuse()
	ctx.MountFuse()
	defer ctx.UnmountFuse()
	ctx.checkMount()

	// Start go routine for checking FUSE connection continuously
	quit := make(chan bool)
	ctx.checkMountRepeat(quit)

	// Construct Rsync parameters
	// Ex: ftp.ncbi.nih.gov/blast/db
	source := fmt.Sprintf("%s%s/", ctx.Server, ctx.SourcePath)
	template := "rsync -arzv -n --itemize-changes --delete " +
		"--size-only --no-motd --exclude '.*' --exclude 'cloud/*' " +
		"--exclude 'nr.gz' --exclude 'nt.gz' --exclude " +
		"'other_genomic.gz' --exclude 'refseq_genomic*' " +
		"--copy-links --prune-empty-dirs %s %s"

	// Dry run
	err = os.MkdirAll(ctx.LocalPath, os.ModePerm)
	if err != nil {
		err = newErr("Couldn't make local path.", err)
		log.Print(err)
		return newF, modified, deleted, err
	}
	cmd := fmt.Sprintf(template, source, ctx.LocalPath)
	log.Print("Beginning dry run execution...")
	stdout, _, err := commandVerboseOnErr(cmd)
	if err != nil {
		return newF, modified, deleted, err
	}

	// FUSE connection no longer needed after this point.
	quit <- true // Terminate FUSE-checking goroutine
	log.Print("Done with dry run...\nParsing changes...")
	newF, modified, deleted = parseChanges(stdout, ctx.SourcePath)
	log.Printf("New on remote: %s", newF)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)
	return newF, modified, deleted, err
}

// Archives a file by moving it from the current storage to the archive
// folder under an archiveKey name and recording it in the db.
func (ctx *Context) moveOldFile(file string) error {
	// Setup
	// Ex: $HOME/remote/blast/db/README
	localPath := ctx.LocalTop + file
	log.Print("Archiving old version of: " + file)
	num := ctx.lastVersionNum(file, false)
	key, err := ctx.generateHash(localPath, file, num)
	if err != nil {
		err = newErr("Error in generating checksum.", err)
		log.Print(err)
		return err
	}

	// Move to archive folder
	svc := s3.New(session.Must(session.NewSession()))
	// Ex: bucket/remote/blast/db/README
	log.Print("Copy from: " + ctx.Bucket + file)
	log.Print("Copy-to key: " + "archive/" + key)

	// Get file size
	size, err := ctx.fileSizeOnS3(file, svc)
	if err != nil {
		return err
	}

	// File operations
	if size < 4500000000 {
		// Handle via S3 SDK
		err = ctx.copyOnS3(file, key, svc)
		if err != nil {
			return err
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
			return err
		}
	}

	// Update the old db entry with archiveKey blob
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Print("Db query: " + query)
	_, err = ctx.Db.Query("update entries set ArchiveKey=? where "+
		"PathName=? and VersionNum=?;", key, file, num)
	if err != nil {
		err = newErr("Error in updating db entry.", err)
		log.Print(err)
	}
	return err
}

// Moves to-be-replaced files to archive folder on S3.
func (ctx *Context) moveOldFiles(files []string) error {
	for _, file := range files {
		ctx.moveOldFile(file)
	}
	return nil
}

// Copies list of files from remote server to local disk folder with rsync.
func (ctx *Context) copyFilesFromRemote(files []string) error {
	for _, file := range files {
		ctx.copyFileFromRemote(file)
	}
	return nil
}

// Copies one file from remote server to local disk folder with rsync.
func (ctx *Context) copyFileFromRemote(file string) error {
	source := fmt.Sprintf("%s%s", ctx.Server, file)
	// Ex: $HOME/tempNew/blast/db
	log.Print("Local dir to make: " + ctx.TempNew + filepath.Dir(file))
	err := os.MkdirAll(ctx.TempNew+filepath.Dir(file), os.ModePerm)
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
	commandStreaming(cmd)
	return nil
}

// Uploads list of files from local disk to S3 folder.
func (ctx *Context) putObjects(files []string) error {
	for _, file := range files {
		ctx.putObject(ctx.TempNew+file, file)
	}
	return nil
}

// Uploads one file from local disk to S3 with uploadKey.
func (ctx *Context) putObject(onDisk string, uploadKey string) error {
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
	os.Remove(onDisk)
	return err
}

// Copies a file on S3 from the CopySource to the archive folder with key.
func (ctx *Context) copyOnS3(file string, key string, svc *s3.S3) error {
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
func (ctx *Context) fileSizeOnS3(file string, svc *s3.S3) (int, error) {
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

// Deletes a list of objects on S3.
func (ctx *Context) deleteObjects(files []string) error {
	for _, file := range files {
		ctx.deleteObject(file)
	}
	return nil
}

// Deletes an object on S3.
func (ctx *Context) deleteObject(file string) error {
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
