package models

import (
	"testing"
	"ncbi_proj/server/utils"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3"
	"fmt"
	"github.com/stretchr/testify/assert"
)

type mockService struct {
	s3iface.S3API
}

func (m mockService) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	temp1 := "Value 1"
	val1 := &temp1
	temp2 := "Value 2"
	val2 := &temp2
	res := &s3.ListObjectsOutput{Contents: []*s3.Object{{Key: val1}, {Key: val2}}}
	return res, nil
}

func TestGet(t *testing.T) {
	ctx := utils.NewContext()
	ctx.Store = mockService{}
	dir := NewDirectory(ctx)
	res, err := dir.Get("Testing")
	if err != nil {
		t.FailNow()
	}
	assert.Equal(t, "[{Value 1} {Value 2}]", fmt.Sprintf("%s", res))
}
