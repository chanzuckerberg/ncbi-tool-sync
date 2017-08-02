package main

import (
	"testing"
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestHandle(t *testing.T) {
	x := errors.New("this is a test error")
	y := handle("hello there", x)
	actual := y.Error()
	expected := "hello there. this is a test error"
	assert.Equal(t, actual, expected)
}

func TestGetUserHome(t *testing.T) {
	actual := getUserHome()
	expected := "/"
	assert.Contains(t, actual, expected)
}

func TestMoveOldFileDb(t *testing.T) {
	mock, ctx := ctxTesting(t)
	result := sqlmock.NewResult(0, 0)
	mock.ExpectExec("update entries").WithArgs("apple", "banana", 2).WillReturnResult(result)

	err := moveOldFileDb(ctx, "apple", "banana", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal("Unfulfilled expections: ", err)
	}
}

func TestGenerateHash(t *testing.T) {
	res, _ := generateHash("Apples", 5)
	assert.Equal(t, "8a17a33a281ac505865be8e1b459b998", res)
}
