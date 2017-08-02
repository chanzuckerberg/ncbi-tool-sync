package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
)

func TestGetPreviousStateTrivial(t *testing.T) {
	_, ctx := ctxTesting(t)
	testServer.Response(200, make(map[string]string), "")
	f := syncFolder{
		sourcePath: "/blast/db",
		flags: []string{},
	}
	output, err := getPreviousState(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	actual := fmt.Sprintf("%s", output)
	expected := "map[]"
	assert.Equal(t, expected, actual)
}

func TestMoveOldFileOperations(t *testing.T) {
	_, ctx := ctxTesting(t)
	testServer.Response(200, make(map[string]string), "")
	testServer.Response(200, make(map[string]string), "")

	err := moveOldFileOperations(ctx, "apple", "banana")
	if err != nil {
		t.Fatal(err)
	}
	testServer.WaitRequest()
}

func TestGetFilteredSet(t *testing.T) {
	_, ctx := ctxTesting(t)
	f := syncFolder{
		sourcePath: "/blast/db",
		flags: []string{},
	}
	commandWithOutput = FakeRsync
	toInspect, folderSet, _ := getFilteredSet(ctx, f)
	actual := toInspect.String()
	assert.Contains(t, actual, "/blast/db/banana")
	assert.Contains(t, actual, "/blast/db/cherry")
	assert.Contains(t, actual, "/blast/db/date")
	actual = folderSet.String()
	assert.Contains(t, actual, "/blast/db/date")
	assert.Contains(t, actual, "/blast/db/cherry")
}

func FakeRsync(cmd string) (string, string, error) {
	stdout := "1 apple\n1 banana\n1 cherry/cranberry\n1 date/dragonfruit\n1 elderberry\n1 fig\n1 grape\n1 huckleberry"
	return stdout, "banana", nil
}

func TestFileOperationStage(t *testing.T) {
	// Setup
	mock, ctx := ctxTesting(t)
	commandWithOutput = FakeRsync
	res := syncResult{}
	res.newF = []string{"apricot", "avocado", "bilberry"}
	res.modified = []string{"currant", "coconut"}
	res.deleted = []string{"durian", "grape"}
	for i := 0; i < 4; i ++ {
		testServer.Response(200, make(map[string]string), "")
	}
	for _, v := range append(res.newF, res.modified...) {
		ctx.os.Create(v)
	}
	for _, v := range append(res.modified, res.deleted...) {
		mock.ExpectQuery("select VersionNum from entries").WithArgs(v).WillReturnRows(testRows)
	}

	// Call
	fileOperationStage(ctx, res)
	testServer.WaitRequest()
}
