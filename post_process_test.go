package main

import (
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

func testSetup(t *testing.T) (sqlmock.Sqlmock, *Context) {
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
	ctx := &Context{
		os:    afero.NewMemMapFs(),
		Db:    db,
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

func TestDbUpdateStage(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apples").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("apples", 1).WillReturnResult(result)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("bananas").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("bananas", 1).WillReturnResult(result)

	// Run test
	res := syncResult{
		[]string{"apples"}, []string{"bananas"}, []string{"cherries"},
	}
	err := dbUpdateStage(ctx, res)
	if err != nil {
		t.Fatal(err)
	}

	// Check expectations
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("Unfulfilled expections: ", err)
	}
}

func FakeGetModTime(pathName string,
	cache map[string]map[string]string) string {
	return "2017-08-02T22:20:26"
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

func TestDbNewVersions(t *testing.T) {
	// Setup
	mock, ctx := testSetup(t)
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apple").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("apple", 1, "2017-08-02T22:20:26").WillReturnResult(result)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("banana").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("banana", 1, "2017-08-02T22:20:26").WillReturnResult(result)
	getModTime = FakeGetModTime
	defer func() { getModTime = getModTimeFTP }()

	// Run test
	err := dbNewVersions(ctx, []string{"apple", "banana"})
	if err != nil {
		t.Fatal(err)
	}

	// Check expectations
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("Unfulfilled expections: ", err)
	}
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

	_, err := getDbModTime(ctx, "pomegranate")
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}
