package controllers

import (
	"github.com/gorilla/mux"
	"ncbi_proj/server/models"
	"ncbi_proj/server/utils"
	"net/http"
)

type DirectoryController struct {
	ApplicationController
	ctx *utils.Context
}

func NewDirectoryController(ctx *utils.Context) *DirectoryController {
	return &DirectoryController{
		ctx: ctx,
	}
}

func (dc *DirectoryController) Register(router *mux.Router) {
	router.HandleFunc("/directory", dc.Show)
}

func (dc *DirectoryController) Show(w http.ResponseWriter, r *http.Request) {
	// Setup
	dir := models.NewDirectory(dc.ctx)
	op := r.URL.Query().Get("op")
	pathName := r.URL.Query().Get("path-name")
	var err error
	var result interface{}

	// Dispatch operations
	switch {
	case pathName == "":
		dc.BadRequest(w)
		return
	case op == "download":
		// Serve up the folder with pre-signed download URLs
		result, err = dir.GetWithURLs(pathName)
	default:
		// Serve up a simple directory listing
		result, err = dir.Get(pathName)
	}

	if err != nil {
		dc.InternalError(w, err)
		return
	}
	dc.Output(w, result)
}
