package main

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

func TestSetupDatabase(t *testing.T) {
	_, ctx := testSetup(t)
	res, _ := setupDatabase(ctx)
	actual := ctx.db
	expected := &sql.DB{}
	assert.IsType(t, actual, expected)
	assert.Contains(t, res, "@tcp(")
}

func TestCreateTable(t *testing.T) {
	mock, ctx := testSetup(t)
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(testResult)
	dbCreateTable(ctx)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func FakeSetupDatabase(ctx *context) (string, error) {
	db, mock, _ := sqlmock.New()
	mock.ExpectClose()
	ctx.db = db
	return "", nil
}

func TestLastVersionNumDb(t *testing.T) {
	mock, ctx := testSetup(t)
	mock.ExpectQuery("select VersionNum from entries").WithArgs("strawberry").WillReturnRows(testRows)
	res := dbLastVersionNum(ctx, "strawberry", false)
	assert.Equal(t, -1, res)

	mock.ExpectQuery("select VersionNum from entries").WithArgs("strawberry").WillReturnRows(testRows)
	res = dbLastVersionNum(ctx, "strawberry", true)
	assert.Equal(t, -1, res)

	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestMoveOldFileDb(t *testing.T) {
	mock, ctx := testSetup(t)
	result := sqlmock.NewResult(0, 0)
	mock.ExpectExec("update entries").WithArgs("banana", "apple", 2).WillReturnResult(result)

	err := dbArchiveFile(ctx, "apple", "banana", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("Unfulfilled expections: ", err)
	}
}
