package main

import (
	"fmt"
	"log"
	"strings"
	"os"
	"time"
	"path/filepath"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"bytes"
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

func (ctx *Context) rsyncCaller() error {
	// FUSE check
	ctx.UnmountFuse()
	ctx.MountFuse()
	defer ctx.UnmountFuse()
	ctx.checkMount()
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
			//log.Println("Mount check successful.")
				time.Sleep(time.Duration(10)*time.Second)
			}
		}
	}()
	return nil
}

// Calls the Rsync workflow. Executes a dry run first for processing.
// Then runs the real sync. Finally processes the changes.
func (ctx *Context) callRsyncFlow() error {
	var err error
	var cmd string

	ctx.UnmountFuse()
	ctx.MountFuse()
	//defer ctx.UnmountFuse()
	ctx.checkMount()
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
				time.Sleep(time.Duration(10)*time.Second)
			}
		}
	}()

	// Construct Rsync parameters
	source := fmt.Sprintf("%s%s/", ctx.Server, ctx.SourcePath)
	tempDir := setTempDir()
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
	cmd = fmt.Sprintf(template, source, ctx.LocalPath)
	log.Println("Beginning dry run execution...")
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

	//// Copy from remote to local temp
	ctx.copyFromRemote(newF)
	ctx.copyFromRemote(modified)

	// Move files around on S3 to be replaced
	ctx.moveOldFiles(modified)
	ctx.moveOldFiles(deleted)

	// Delete files to be deleted on S3
	ctx.deleteObjects(modified)
	ctx.deleteObjects(deleted)

	// Upload new files
	ctx.putObjects(newF)
	ctx.putObjects(modified)

	// Actual run
	//log.Println("Beginning actual sync run execution...")
	//template = "rsync -abrzv %s --delete " +
	//	"--size-only --no-motd --progress --stats -h --exclude '.*' --exclude 'cloud/*' --exclude 'nr.gz' --exclude 'nt.gz' --exclude 'other_genomic.gz' --exclude 'refseq_genomic*' --copy-links --prune-empty-dirs --backup-dir='%s' %s %s"
	//cmd = fmt.Sprintf(template, "", tempDir, source, ctx.LocalPath)
	//commandStreaming(cmd)

	// Process changes
	log.Println("Done with real run...\nProcessing changes...")
	err = ctx.processChanges(newF, modified, tempDir)
	log.Println("Finished processing changes.")
	quit <- true
	return err
}

func (ctx *Context) moveOldFile(file string) error {
	localPath := ctx.LocalTop + file
	log.Println("Archiving old version of: "+ file)
	num := ctx.lastVersionNum(file, false)
	key, err := ctx.generateHash(localPath, file, num)
	if err != nil {
		log.Println("err: " + err.Error())
		return err
	}

	// Move to archive folder
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	log.Println("Copy from: " + ctx.Bucket + file)
	log.Println("Copy to: " + "archive/" + key)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(ctx.Bucket),
		CopySource: aws.String(ctx.Bucket + file),
		Key:        aws.String("archive/" + key),
	}
	result, err := svc.CopyObject(input)
	log.Println(result)
	if err != nil {
		log.Println("err: " + err.Error())
	}

	// Update the old entry with archiveKey blob
	query := fmt.Sprintf(
		"update entries set ArchiveKey='%s' where "+
			"PathName='%s' and VersionNum=%d;", key, file, num)
	log.Println("Db query: " + query)
	_, err = ctx.Db.Exec(query)
	if err != nil {
		log.Println("Error in updating entry: " + err.Error())
	}
	return nil
}

func (ctx *Context) moveOldFiles(files []string) error {
	for _, file := range files {
		ctx.moveOldFile(file)
	}
	return nil
}

func (ctx *Context) copyFromRemote(files []string) error {
	for _, file := range files {
		// mkdir all
		source := fmt.Sprintf("%s%s", ctx.Server, file)
		log.Println("Path to make: " + ctx.TempNew + filepath.Dir(file))
		os.MkdirAll(ctx.TempNew + filepath.Dir(file), os.ModePerm)
		dest := fmt.Sprintf("%s%s", ctx.TempNew, file)
		template := "rsync -arzv " +
			"--size-only --no-motd --progress --copy-links %s %s"
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

func (ctx *Context) putObject(file string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	source := fmt.Sprintf("%s%s", ctx.TempNew, file)
	fmt.Println("Source: " + source)
	actualFile, _ := os.Open(source)
	defer actualFile.Close()
	fileInfo, _ := actualFile.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	actualFile.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	input := &s3.PutObjectInput{
		Body:                 fileBytes,
		Bucket:               aws.String(ctx.Bucket),
		Key:                  aws.String(file),
	}
	result, err := svc.PutObject(input)
	fmt.Println(result)
	if err != nil {
		fmt.Println(err.Error())
	}

	return nil
}

func (ctx *Context) deleteObjects(files []string) error {
	for _, file := range files {
		ctx.deleteObject(file)
	}
	return nil
}

func (ctx *Context) deleteObject(file string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))
	svc := s3.New(sess)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(ctx.Bucket),
		Key:    aws.String(file),
	}

	result, err := svc.DeleteObject(input)
	fmt.Println(result)
	if err != nil {
		fmt.Println(err.Error())
	}
	return nil
}
