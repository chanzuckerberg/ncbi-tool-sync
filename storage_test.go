package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"database/sql"
)

func TestSetupDatabase(t *testing.T) {
	_, ctx := testSetup(t)
	res, _ := setupDatabase(ctx)
	actual := ctx.Db
	expected := &sql.DB{}
	assert.IsType(t, actual, expected)
	assert.Contains(t,  res,"@tcp(")
}