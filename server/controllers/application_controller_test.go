package controllers

import (
	"github.com/stretchr/testify/assert"
	"ncbi_proj/server/models"
	"ncbi_proj/server/utils"
	"net/http/httptest"
	"testing"
)

func TestOutput(t *testing.T) {
	ctx := utils.NewContext()
	ac := NewApplicationController(ctx)
	entry := models.Entry{"blast", 5, "2009-09-29T14:24:20Z", ""}
	w := httptest.NewRecorder()
	ac.Output(w, entry)
	assert.Equal(t, `{"Path":"blast","Version":5,"ModTime":"2009-09-29T14:24:20Z"}`, w.Body.String())
}
