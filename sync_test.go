package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
	"github.com/AdRoll/goamz/testutil"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"log"
	"github.com/jlaffaye/ftp"
)

func TestGetPreviousStateTrivial(t *testing.T) {
	_, ctx := testSetup(t)
	expectResponse(testServer, 1)
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
	_, ctx := testSetup(t)
	expectResponse(testServer, 2)

	err := moveOldFileOperations(ctx, "apple", "banana")
	if err != nil {
		t.Fatal(err)
	}
	testServer.WaitRequest()
}

func TestGetFilteredSet(t *testing.T) {
	_, ctx := testSetup(t)
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
	m, ctx := testSetup(t)
	commandWithOutput = FakeRsync
	lastVersionNum = FakeLastVersionNum
	expectResponse(testServer, 10)
	res := syncResult{}
	res.newF = []string{"apricot", "avocado", "bilberry"}
	res.modified = []string{"currant", "coconut"}
	res.deleted = []string{"durian", "grape"}
	expectResponse(testServer, 4)
	for _, v := range append(res.newF, res.modified...) {
		ctx.os.Create(v)
	}
	expectSet(m, "b072f43c1e66bb2e13e5115df39b7db0", "currant")
	expectSet(m, "f80d7121c31e219bfa56268befb11c43", "coconut")
	expectSet(m, "83b00e161b904636d64826336c95ba9f", "durian")
	expectSet(m, "eb2d7c30e19f867b987928086b7a6e56", "grape")
	for _, v := range append(res.modified, res.deleted...) {
		m.ExpectQuery("select VersionNum from entries").WithArgs(v).WillReturnRows(testRows)
	}

	// Call
	fileOperationStage(ctx, res)
	testServer.WaitRequest()
	m.ExpectationsWereMet()
}

func expectSet(mock sqlmock.Sqlmock, blob string, name string) {
	mock.ExpectExec("update entries").WithArgs(blob, name, 2).WillReturnResult(testResult)
}

func expectResponse(server *testutil.HTTPServer, count int) {
	for i := 0; i < count; i ++ {
		server.Response(200, make(map[string]string), "")
	}
}

func TestDryRunStage(t *testing.T) {
	// Setup
	_, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
		flags: []string{},},
	}
	commandWithOutput = FakeRsync
	expectResponse(testServer, 4)

	// Call
	res, _ := dryRunStage(ctx)
	testServer.WaitRequest()
	actual := fmt.Sprint(res)
	expected := "{[] [] []}"
	assert.Equal(t, expected, actual)
}

func FakeLastVersionNum(ctx *Context, file string, inclArchive bool) int {
	return 2
}

func TestMoveOldFile(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	lastVersionNum = FakeLastVersionNum
	expectResponse(testServer, 2)
	mock.ExpectExec("update entries").WithArgs("c979e64a41a4789aaaa5c7ee819c45d5", "apples", 2).WillReturnResult(testResult)

	// Call
	err := moveOldFile(ctx, "apples")
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestDryRunStageWithFake(t *testing.T) {
	_, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
			flags: []string{},}, }
	getChanges = FakeGetChanges
	res, err := dryRunStage(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "{[lemon] [lime] [mango]}", fmt.Sprint(res))
}

func FakeGetChanges(ctx *Context, folder syncFolder) (syncResult, error) {
	res := syncResult{}
	log.Println("hello there")
	res.newF = []string{"lemon"}
	res.modified = []string{"lime"}
	res.deleted = []string{"mango"}
	return res, nil
}

func FakeClientList(client *ftp.ServerConn, dir string) ([]*ftp.Entry, error) {
	res := ftp.Entry{}
	return []*ftp.Entry{&res}, nil
}

func TestCallSyncFlow(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	ctx.syncFolders = []syncFolder{
		{sourcePath: "/apple/berry",
		flags: []string{},},
	}
	expectResponse(testServer, 8)
	for _, v := range []string{"lemon", "lime"} {
		ctx.os.Create(v)
	}
	mock.ExpectExec("update entries").WithArgs("03dbc4e3e7436484db322c0efaffe23d", "lime", 2).WillReturnResult(testResult)
	mock.ExpectExec("update entries").WithArgs("705c18ec390c3520692680d24d6f8d78", "mango", 2).WillReturnResult(testResult)
	mock.ExpectExec("insert into entries").WithArgs("lemon", 3).WillReturnResult(testResult)
	mock.ExpectExec("insert into entries").WithArgs("lime", 3).WillReturnResult(testResult)

	// Call
	callSyncFlow(ctx, false)
	testServer.WaitRequest()
	assert.Nil(t, mock.ExpectationsWereMet())
}