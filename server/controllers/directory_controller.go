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
	inputTime := r.URL.Query().Get("input-time")
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
		// Serve up the folder with download URLs
		result, err = dir.GetWithURLs(pathName)
	case op == "at-time":
		// Serve up a simple folder listing at a given time
		result, err = dir.GetAtTime(pathName, inputTime)
	case op == "download-at-time":
		// Serve up folder at given time with download URLs
		result, err = dir.GetAtTimeWithURLs(pathName, inputTime)
	default:
		// Serve up a simple folder listing
		result, err = dir.GetLatest(pathName)
	}

	if err != nil {
		dc.InternalError(w, err)
		return
	}
	dc.Output(w, result)
}
