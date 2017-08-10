package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
