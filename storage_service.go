package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
	"strings"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var fileSizeOnS3 = fileSizeOnS3Svc

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