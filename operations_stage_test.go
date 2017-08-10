package main

import (
	"fmt"
	"github.com/AdRoll/goamz/testutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"log"
	"os"
	"testing"
	"time"
)

var testServer *testutil.HTTPServer

func init() {
	testServer = serverTesting()
}

func testSetup(t *testing.T) (sqlmock.Sqlmock, *context) {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	sess := session.Must(session.NewSession())
	region := "us-west-2"
	sess.Config.Region = &region
	ctx := &context{
		os:    afero.NewMemMapFs(),
		db:    db,
		svcS3: s3.New(sess),
	}
	ctx.svcS3.Endpoint = testServer.URL
	clientList = FakeClientList
	return mock, ctx
}

func serverTesting() (serv *testutil.HTTPServer) {
	url := "http://localhost:4445"
	serv = &testutil.HTTPServer{URL: url, Timeout: 2 * time.Second}
	serv.Start()
	return serv
}

func TestGetModTimeFTP(t *testing.T) {
	testSetup(t)
	pathName := "testFolder/testFile"
	cache := make(map[string]map[string]string)
	res := getModTimeFTP(pathName, cache)
	assert.Equal(t, "2017-08-04T22:08:41", res)
	t2 := "2017-08-15T22:08:41"
	cache["testFolder"]["testFile"] = t2
	res = getModTimeFTP(pathName, cache)
	assert.Equal(t, t2, res)
}

func TestDbNewVersion(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apple").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("apple", 1).WillReturnResult(result)

	// Run test
	cache := make(map[string]map[string]string)
	err := dbNewVersion(ctx, "apple", cache)
	if err != nil {
		t.Fatal(err)
	}

	// Check expectations
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("Unfulfilled expections: ", err)
	}
}

func TestGetDbModTime(t *testing.T) {
	mock, ctx := testSetup(t)
	mock.ExpectQuery("select DateModified from entries").WithArgs("pomegranate").WillReturnRows(testRows)

	_, err := dbGetModTime(ctx, "pomegranate")
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

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

func TestMoveOldFileOperationsLarge(t *testing.T) {
	_, ctx := testSetup(t)
	expectResponse(testServer, 2)

	err := moveObject(ctx, "apple", "12345")
	if err != nil {
		t.Fatal(err)
	}
	testServer.WaitRequest()
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
