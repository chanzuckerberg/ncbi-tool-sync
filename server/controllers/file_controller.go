package controllers

import (
	"github.com/gorilla/mux"
	"ncbi_proj/server/models"
	"ncbi_proj/server/utils"
	"net/http"
)

type FileController struct {
	ApplicationController
	ctx *utils.Context
}

func NewFileController(ctx *utils.Context) *FileController {
	return &FileController{
		ctx: ctx,
	}
}

func (fc *FileController) Register(router *mux.Router) {
	router.HandleFunc("/file", fc.Show)
}

func (fc *FileController) Show(w http.ResponseWriter, r *http.Request) {
	// Setup
	file := models.NewFile(fc.ctx)
	op := r.URL.Query().Get("op")
	pathName := r.URL.Query().Get("path-name")
	var err error
	var result interface{}
	versionNum := r.URL.Query().Get("version-num")

	// Dispatch operations
	switch {
	case pathName == "":
		fc.BadRequest(w)
		return
	case op == "history":
		// Serve up file history
		result, err = file.GetHistory(pathName)
	default:
		// Serve up the file
		result, err = file.Get(pathName, versionNum)
	}

	if err != nil {
		fc.Error(w, err)
		return
	}
	fc.Output(w, result)
}
