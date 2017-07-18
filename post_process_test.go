package main

import (
	"testing"
	"github.com/spf13/afero"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"log"
	"os"
)

func setup(t *testing.T) (sqlmock.Sqlmock, *Context) {
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
	}
	return mock, ctx
}

func TestDbUpdateStage(t *testing.T) {
	// Setup
	mock, ctx := setup(t)
	rows := sqlmock.NewRows([]string{""})
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apples").WillReturnRows(rows)
	mock.ExpectExec("insert into entries").WithArgs("apples", 1).WillReturnResult(result)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("bananas").WillReturnRows(rows)
	mock.ExpectExec("insert into entries").WithArgs("bananas", 1).WillReturnResult(result)

	// Run test
	err := dbUpdateStage(ctx, []string{"apples"}, []string{"bananas"})
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
	mock, ctx := setup(t)
	rows := sqlmock.NewRows([]string{""})
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apple").WillReturnRows(rows)
	mock.ExpectExec("insert into entries").WithArgs("apple", 1).WillReturnResult(result)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("banana").WillReturnRows(rows)
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
	mock, ctx := setup(t)
	rows := sqlmock.NewRows([]string{""})
	result := sqlmock.NewResult(0, 0)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("apple").WillReturnRows(rows)
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