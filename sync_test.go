package main

import (
	"errors"
	"fmt"
	"github.com/AdRoll/goamz/testutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jlaffaye/ftp"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
	"time"
)

func TestGetPreviousStateTrivial(t *testing.T) {
	_, ctx := testSetup(t)
	expectResponse(testServer, 1)
	f := syncFolder{
		sourcePath: "/blast/db",
		flags:      []string{},
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
	_, ctx := testSetup(t)
	expectResponse(testServer, 2)
	tmp := fileSizeOnS3
	fileSizeOnS3 = FakeFileSizeOnS3
	defer func() { fileSizeOnS3 = tmp }()
	commandWithOutput = FakeRsync

	err := moveObject(ctx, "apple", "12345")
	assert.Nil(t, err)

	commandWithOutput = FakeCmdWithError
	err = moveObject(ctx, "apple", "12345")
	assert.NotNil(t, err)
	commandWithOutput = commandWithOutputFunc
}

func FakeFileSizeOnS3(ctx *context, file string, svc *s3.S3) (int, error) {
	return 5000000000, nil
}

func FakeCmdWithError(cmd string) (string, string, error) {
	return "peach", "pear", errors.New("This SHOULD error")
}

func TestMoveOldFileOperationsLarge(t *testing.T) {
	_, ctx := testSetup(t)
	expectResponse(testServer, 2)

	err := moveObject(ctx, "apple", "12345")
	if err != nil {
		t.Fatal(err)
	}
	testServer.WaitRequest()
}

func TestGetFilteredSet(t *testing.T) {
	_, ctx := testSetup(t)
	f := syncFolder{
		sourcePath: "/blast/db",
		flags:      []string{},
	}
	tmp := commandWithOutput
	commandWithOutput = FakeRsync
	defer func() { commandWithOutput = tmp }()
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
	m, ctx := testSetup(t)
	tmp := commandWithOutput
	commandWithOutput = FakeRsync
	defer func() { commandWithOutput = tmp }()
	tmp2 := lastVersionNum
	lastVersionNum = FakeLastVersionNum
	defer func() { lastVersionNum = tmp2 }()
	tmp3 := getModTime
	getModTime = FakeGetModTime
	defer func() { getModTime = tmp3 }()

	expectResponse(testServer, 10)
	res := syncResult{}
	res.newF = []string{"avocado", "apricot", "bilberry"}
	res.modified = []string{"currant", "coconut"}
	res.deleted = []string{"durian", "grape"}
	expectResponse(testServer, 4)
	for _, v := range append(res.newF, res.modified...) {
		ctx.os.Create(v)
	}
	for _, v := range res.newF {
		expectInsert(m, v)
	}
	expectSet(m, "b072f43c1e66bb2e13e5115df39b7db0", "currant")
	expectInsert(m, "currant")
	expectSet(m, "f80d7121c31e219bfa56268befb11c43", "coconut")
	expectInsert(m, "coconut")
	expectSet(m, "83b00e161b904636d64826336c95ba9f", "durian")
	expectSet(m, "eb2d7c30e19f867b987928086b7a6e56", "grape")

	// Call
	fileOperationStage(ctx, res)
	testServer.WaitRequest()
	m.ExpectationsWereMet()
}

func expectInsert(mock sqlmock.Sqlmock, name string) {
	mock.ExpectExec("insert into entries").WithArgs(name, 3, "2017-08-02T22:20:26").WillReturnResult(testResult)
}

func expectSet(mock sqlmock.Sqlmock, blob string, name string) {
	mock.ExpectExec("update entries").WithArgs(blob, name, 2).WillReturnResult(testResult)
}

func expectResponse(server *testutil.HTTPServer, count int) {
	for i := 0; i < count; i++ {
		server.Response(200, make(map[string]string), "")
	}
}

func TestDryRunStage(t *testing.T) {
	// Setup
	_, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
			flags: []string{}},
	}

	tmp := commandWithOutput
	commandWithOutput = FakeRsync
	defer func() { commandWithOutput = tmp }()
	expectResponse(testServer, 4)

	// Call
	res, _ := dryRunStage(ctx)
	testServer.WaitRequest()
	actual := fmt.Sprint(res)
	expected := "{[] [] []}"
	assert.Equal(t, expected, actual)
}

func FakeLastVersionNum(ctx *context, file string, inclArchive bool) int {
	return 2
}

func TestDryRunStageWithFake(t *testing.T) {
	_, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
			flags: []string{}}}
	tmp := getChanges
	getChanges = FakeGetChanges
	defer func() { getChanges = tmp }()
	res, err := dryRunStage(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "{[lemon] [lime] [mango]}", fmt.Sprint(res))
}

func FakeGetChanges(ctx *context, folder syncFolder) (syncResult, error) {
	res := syncResult{}
	res.newF = []string{"lemon"}
	res.modified = []string{"lime"}
	res.deleted = []string{"mango"}
	return res, nil
}

func FakeClientList(client *ftp.ServerConn, dir string) ([]*ftp.Entry, error) {
	t, _ := time.Parse(time.RFC3339, "2017-08-04T22:08:41+00:00")
	res := ftp.Entry{Name: "testFile", Size: uint64(4000), Time: t}
	return []*ftp.Entry{&res}, nil
}

func TestCallSyncFlow(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
			flags: []string{}},
	}
	tmp := commandWithOutput
	commandWithOutput = FakeRsync
	defer func() { commandWithOutput = tmp }()
	tmp2 := getChanges
	getChanges = FakeGetChanges
	defer func() { getChanges = tmp2 }()
	tmp3 := lastVersionNum
	lastVersionNum = FakeLastVersionNum
	defer func() { lastVersionNum = tmp3 }()

	expectResponse(testServer, 8)
	for _, v := range []string{"lemon", "lime"} {
		ctx.os.Create(v)
	}
	mock.ExpectExec("insert into entries").WithArgs("lemon", 3).WillReturnResult(testResult)
	mock.ExpectExec("update entries").WithArgs("03dbc4e3e7436484db322c0efaffe23d", "lime", 2).WillReturnResult(testResult)
	mock.ExpectExec("insert into entries").WithArgs("lime", 3).WillReturnResult(testResult)
	mock.ExpectExec("update entries").WithArgs("705c18ec390c3520692680d24d6f8d78", "mango", 2).WillReturnResult(testResult)

	// Call
	callSyncFlow(ctx, false)
	testServer.WaitRequest()
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestFileChangeLogic(t *testing.T) {
	// Setup
	pastState := make(map[string]fInfo)
	newState := make(map[string]fInfo)
	s := "raisin"
	pastState[s] = fInfo{
		s, "2017-08-01T20:20:23", 2,
	}
	s = "cucumber"
	pastState[s] = fInfo{
		s, "2017-08-02T20:20:23", 3,
	}
	s = "orange"
	pastState[s] = fInfo{
		s, "2017-08-03T20:20:23", 4,
	}
	newState[s] = pastState[s]
	s = "raspberry.md5"
	pastState[s] = fInfo{
		s, "2017-08-04T20:20:23", 5,
	}
	newState[s] = fInfo{
		s, "2017-08-15T20:20:23", 5,
	}
	s = "raisin"
	newState[s] = fInfo{
		s, "2017-08-05T20:20:23", 6,
	}
	s = "honeydew"
	newState[s] = fInfo{
		s, "2017-08-06T20:20:23", 7,
	}
	s = "fig"
	newState[s] = fInfo{
		s, "2017-08-07T20:20:23", 8,
	}
	names := []string{"raisin", "cucumber", "honeydew", "orange", "fig", "raspberry", "raspberry.md5"}

	// Call
	res := fileChangeLogic(pastState, newState, names)
	assert.EqualValues(t, []string{"honeydew", "fig"}, res.newF)
	assert.EqualValues(t, []string{"raisin", "raspberry.md5"}, res.modified)
	assert.NotContains(t, res.modified, "orange")
	assert.EqualValues(t, []string{"cucumber"}, res.deleted)
}
