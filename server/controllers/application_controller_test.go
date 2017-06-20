package controllers

import (
	"testing"
	"ncbi_proj/server/utils"
	"ncbi_proj/server/models"
	"net/http/httptest"
	"github.com/stretchr/testify/assert"
)

func TestOutput(t *testing.T) {
	ctx := utils.NewContext()
	ac := NewApplicationController(ctx)
	entry := models.VersionEntry{"blast", 5, "2009-09-29T14:24:20Z"}
	w := httptest.NewRecorder()
	ac.Output(w, entry)
	assert.Equal(t, `{"Path":"blast","Version":5,"ModTime":"2009-09-29T14:24:20Z"}`, w.Body.String())
}
