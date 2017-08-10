package main

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

func TestHandle(t *testing.T) {
	x := errors.New("This SHOULD error!")
	y := handle("hello there", x)
	actual := y.Error()
	expected := "hello there. This SHOULD error!"
	assert.Equal(t, expected, actual)
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

func TestGenerateChecksum(t *testing.T) {
	res, _ := generateChecksum("Apples", 5)
	assert.Equal(t, "8a17a33a281ac505865be8e1b459b998", res)
}

func FakeCmd(str string) (string, string, error) {
	err := errors.New("this SHOULD error")
	return "standard output", "standard error", err
}

func TestCommandVerboseOnErr(t *testing.T) {
	commandWithOutput = FakeCmd
	stdout, stderr, err := commandVerboseOnErr("ls")
	assert.Equal(t, "standard output", stdout)
	assert.Equal(t, "standard error", stderr)
	assert.Equal(t, "this SHOULD error", err.Error())
	commandWithOutput = commandWithOutputFunc
}

func TestCommandWithOutputFunc(t *testing.T) {
	stdout, stderr, err := commandWithOutputFunc("echo 'testing!'")
	assert.Equal(t, "testing!\n", stdout)
	assert.Equal(t, "+ echo 'testing!'\n", stderr)
	assert.Nil(t, err)
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
