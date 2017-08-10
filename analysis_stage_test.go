package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

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

func FakeLastVersionNum(ctx *context, file string, inclArchive bool) int {
	return 2
}

func FakeGetChanges(ctx *context, folder syncFolder) (syncResult, error) {
	res := syncResult{}
	res.newF = []string{"lemon"}
	res.modified = []string{"lime"}
	res.deleted = []string{"mango"}
	return res, nil
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
