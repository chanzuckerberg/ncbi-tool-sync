package main

import (
	"github.com/spf13/afero"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"log"
	"os"
	"testing"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/AdRoll/goamz/testutil"
	"time"
)

var testServer *testutil.HTTPServer

func init() {
	testServer = serverTesting()
}

func ctxTesting(t *testing.T) (sqlmock.Sqlmock, *Context) {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := &Context{
		os: afero.NewMemMapFs(),
		Db: db,
		svcS3: s3.New(session.Must(session.NewSession())),
	}
	ctx.svcS3.Endpoint = testServer.URL
	return mock, ctx
}

func serverTesting() (serv *testutil.HTTPServer) {
	url := "http://localhost:4444"
	serv = &testutil.HTTPServer{URL: url, Timeout: 2 * time.Second}
	serv.Start()
	return serv
}

func TestDbUpdateStage(t *testing.T) {
	// Setup
	mock, ctx := ctxTesting(t)
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

func TestDbNewVersions(t *testing.T) {
	// Setup
	mock, ctx := ctxTesting(t)
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apple").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("apple", 1).WillReturnResult(result)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("banana").WillReturnRows(testRows)
	mock.ExpectExec("insert into entries").WithArgs("banana", 1).WillReturnResult(result)

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
	mock, ctx := ctxTesting(t)
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
