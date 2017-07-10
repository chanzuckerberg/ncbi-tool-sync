package main

import (
	"fmt"
	"log"
	"strings"
	"os"
	"path/filepath"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"bytes"
	"time"
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

	// Dry run analysis stage
	newF, modified, deleted, err := ctx.dryRunStage()
	if err != nil {
		log.Println("Error in running dry run. "+err.Error())
		return err
	}

	// File operation stage. Moving actual files around.
	ctx.fileOperationStage(newF, modified, deleted)

	// Db operation stage. Process changes in the db entries.
	err = ctx.dbUpdateStage(newF, modified)
	if err != nil {
		log.Println("Error in processing db changes. "+err.Error())
	}
	log.Println("Finished processing changes.")
	return err
}

func (ctx *Context) fileOperationStage(newF []string, modified []string, deleted []string) {
	log.Println("Beginning file operations stage.")

	// Copy from NCBI remote to local temp folder
	ctx.copyFromRemote(newF)
	ctx.copyFromRemote(modified)

	// Move files around on S3 to be replaced. Moves the to-be-replaced
	// files to the archive folder on S3 and renames them.
	ctx.moveOldFiles(modified)
	ctx.moveOldFiles(deleted)

	// Delete files to-be-deleted on S3
	ctx.deleteObjects(modified)
	ctx.deleteObjects(deleted)

	// Upload new files saved locally to S3
	ctx.putObjects(newF)
	ctx.putObjects(modified)

	// Delete local temp folder
	err := os.RemoveAll(ctx.TempNew)
	if err != nil {
		log.Println("Error removing path. "+err.Error())
	}
}

func (ctx *Context) dryRunStage() ([]string, []string, []string, error) {
	var err error
	log.Println("Beginning dry run stage.")
	ctx.UnmountFuse()
	ctx.MountFuse()
	defer ctx.UnmountFuse()
	ctx.checkMount()

	// Start go routine for checking FUSE connection
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <- quit:
				return
			default:
				stdout, stderr, err := commandWithOutput("ls " + ctx.LocalTop)
				if strings.Contains(stderr, "endpoint is not connected") || strings.Contains(stderr, "is not a mountpoint") || err != nil {
					log.Println(stdout)
					log.Println(stderr)
					log.Println("Can't connect to mount point.")
					ctx.UnmountFuse()
					ctx.MountFuse()
				}
				time.Sleep(time.Duration(5)*time.Second)
			}
		}
	}()

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
		log.Println("Couldn't make local path: "+err.Error())
	}
	cmd := fmt.Sprintf(template, source, ctx.LocalPath)
	log.Println("Beginning dry run execution...")
	stdout, _, err := commandVerbose(cmd)
	if err != nil {
		log.Println(err)
		log.Fatal("Error in running dry run.")
	}

	// FUSE connection no longer needed after this point.
	quit <- true // Terminate FUSE-checking goroutine
	log.Println("Done with dry run...\nParsing changes...")
	newF, modified, deleted := parseChanges(stdout, ctx.SourcePath)
	log.Printf("New on remote: %s", newF)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)
	return []string{}, []string{}, []string{}, err
}

func (ctx *Context) moveOldFile(file string) error {
	// Setup
	// Ex: $HOME/remote/blast/db/README
	localPath := ctx.LocalTop + file
	log.Println("Archiving old version of: "+file)
	num := ctx.lastVersionNum(file, false)
	key, err := ctx.generateHash(localPath, file, num)
	if err != nil {
		log.Println("Error in generating checksum. " + err.Error())
		return err
	}

	// Move to archive folder
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	// Ex: bucket/remote/blast/db/README
	log.Println("Copy from: " + ctx.Bucket + file)
	log.Println("Copy-to key: " + "archive/" + key)
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(ctx.Bucket),
		CopySource: aws.String(ctx.Bucket + file),
		Key:        aws.String("archive/" + key),
	}
	output, err := svc.CopyObject(params)
	log.Println(output)
	if err != nil {
		log.Println(fmt.Sprintf("Error in copying %s on S3. %s", file, err.Error()))
		return err
	}

	// Update the old db entry with archiveKey blob
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Println("Db query: " + query)
	_, err = ctx.Db.Exec(query)
	if err != nil {
		log.Println("Error in updating db entry. " + err.Error())
	}
	return err
}

func (ctx *Context) moveOldFiles(files []string) error {
	for _, file := range files {
		ctx.moveOldFile(file)
	}
	return nil
}

func (ctx *Context) copyFromRemote(files []string) error {
	for _, file := range files {
		source := fmt.Sprintf("%s%s", ctx.Server, file)
		// Ex: $HOME/tempNew/blast/db
		log.Println("Local path to make: " + ctx.TempNew + filepath.Dir(file))
		err := os.MkdirAll(ctx.TempNew + filepath.Dir(file), os.ModePerm)
		if err != nil {
			log.Println("Couldn't make dir. " + err.Error())
			return err
		}
		// Ex: $HOME/tempNew/blast/db/README
		dest := fmt.Sprintf("%s%s", ctx.TempNew, file)
		template := "rsync -arzv --size-only --no-motd --progress " +
			"--copy-links %s %s"
		cmd := fmt.Sprintf(template, source, dest)
		commandStreaming(cmd)
	}
	return nil
}

func (ctx *Context) putObjects(files []string) error {
	for _, file := range files {
		ctx.putObject(file)
	}
	return nil
}

func (ctx *Context) putObject(filePath string) error {
	// Setup
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	// Ex: $HOME/tempNew/blast/db/README
	source := fmt.Sprintf("%s%s", ctx.TempNew, filePath)
	log.Println("Source: " + source)
	local, err := os.Open(source)
	if err != nil {
		log.Println("Error in opening file on disk. " + err.Error())
		return err
	}
	defer local.Close()
	info, err := local.Stat()
	if err != nil {
		log.Println("Error in reading file on disk. " + err.Error())
		return err
	}

	// Upload file to S3
	size := info.Size()
	buffer := make([]byte, size)
	local.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	input := &s3.PutObjectInput{
		Body:                 fileBytes,
		Bucket:               aws.String(ctx.Bucket),
		Key:                  aws.String(filePath),
	}
	output, err := svc.PutObject(input)
	log.Println(output)
	if err != nil {
		log.Println(fmt.Sprintf("Error in uploading %s to S3. %s", source, err.Error()))
	}
	return err
}

func (ctx *Context) deleteObjects(files []string) error {
	for _, file := range files {
		ctx.deleteObject(file)
	}
	return nil
}

func (ctx *Context) deleteObject(file string) error {
	// Setup
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(file),
	}

	output, err := svc.DeleteObject(input)
	log.Println(output)
	if err != nil {
		log.Println("Error in deleting object. "+err.Error())
	}
	return err
}
