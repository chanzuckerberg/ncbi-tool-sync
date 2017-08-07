package main

import (
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

var testRows = sqlmock.NewRows([]string{""})
var testResult = sqlmock.NewResult(1, 1)

func TestMainCall(t *testing.T) {
	testSetup(t)
	setupDatabase = FakeSetupDatabase
	callSyncFlow = FakeCallSyncFlow
	main()

	setupDatabase = setupDatabaseWithCtx
	callSyncFlow = callSyncFlowRepeat
}

func FakeSetupDatabase(ctx *context) (string, error) {
	db, mock, _ := sqlmock.New()
	mock.ExpectClose()
	ctx.Db = db
	return "", nil
}

func FakeCallSyncFlow(ctx *context, repeat bool) error {
	return nil
}
