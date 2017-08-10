package main

import "github.com/aws/aws-sdk-go/service/s3"

func FakeFileSizeOnS3(ctx *context, file string, svc *s3.S3) (int, error) {
	return 5000000000, nil
}
