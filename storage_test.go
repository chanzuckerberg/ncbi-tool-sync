package main

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetupDatabase(t *testing.T) {
	_, ctx := testSetup(t)
	res, _ := setupDatabase(ctx)
	actual := ctx.Db
	expected := &sql.DB{}
	assert.IsType(t, actual, expected)
	assert.Contains(t, res, "@tcp(")
}

func TestCreateTable(t *testing.T) {
	mock, ctx := testSetup(t)
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(testResult)
	CreateTable(ctx)
	assert.Nil(t, mock.ExpectationsWereMet())
}
