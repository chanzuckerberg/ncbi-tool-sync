package models

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"ncbi_proj/server/utils"
	"testing"
)

type mockService struct {
	s3iface.S3API
}

func (m mockService) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	temp1 := "Value 1"
	val1 := &temp1
	temp2 := "Value 2"
	val2 := &temp2
	num := int64(5)
	res := &s3.ListObjectsOutput{Contents: []*s3.Object{{Key: val1, Size: &num}, {Key: val2, Size: &num}}}
	return res, nil
}

func (m mockService) HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	res := &s3.HeadObjectOutput{}
	return res, errors.New("bla")
}

func TestGet(t *testing.T) {
	ctx := utils.NewContext()
	ctx.Store = mockService{}
	dir := NewDirectory(ctx)
	res, err := dir.GetLatest("Testing", "")
	if err != nil {
		t.Logf(err.Error())
	}
	assert.Equal(t, "[{Value 1 %!s(int=0)  } {Value 2 %!s(int=0)  }]", fmt.Sprintf("%s", res))
}
