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

func NewDirectoryController(
	ctx *utils.Context) *DirectoryController {
	return &DirectoryController{
		ctx: ctx,
	}
}

func (dc *DirectoryController) Register(router *mux.Router) {
	router.HandleFunc("/directory", dc.Show)
}

func (dc *DirectoryController) Show(w http.ResponseWriter,
	r *http.Request) {
	// Setup
	dir := models.NewDirectory(dc.ctx)
	inputTime := r.URL.Query().Get("input-time")
	op := r.URL.Query().Get("op")
	pathName := r.URL.Query().Get("path-name")
	output := r.URL.Query().Get("output")
	var err error
	var result interface{}

	// Dispatch operations
	switch {
	case pathName == "":
		dc.BadRequest(w)
		return
	case op == "at-time":
		// Serve up folder at a given time
		result, err = dir.GetPast(pathName, inputTime, output)
	default:
		// Serve up latest version of the folder
		result, err = dir.GetLatest(pathName, output)
	}

	if err != nil {
		dc.InternalError(w, err)
		return
	}
	dc.Output(w, result)
}
