package controllers

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"io"
	"ncbi_proj/server/models"
	"ncbi_proj/server/utils"
	"net/http"
)

type FileControllerImpl struct {
	ctx *utils.Context
}

func NewFileController(ctx *utils.Context) *FileControllerImpl {
	return &FileControllerImpl{
		ctx: ctx,
	}
}

func (fc *FileControllerImpl) Register(router *mux.Router) {
	router.HandleFunc("/file", fc.Show)
}

func (fc *FileControllerImpl) Show(w http.ResponseWriter, r *http.Request) {
	// Setup
	pathName := r.URL.Query().Get("path-name")
	versionNum := r.URL.Query().Get("version-num")
	op := r.URL.Query().Get("op")
	file := new(models.File)

	if pathName != "" && op == "" {
		// Serve up the file
		result, err := file.Get(pathName, versionNum, fc.ctx)
		if err != nil {
			fc.Error(w, err)
			return
		}
		fc.Output(w, result)
	} else if op == "history" {
		// Serve up file history
		file.GetHistory(pathName)
	} else {
		io.WriteString(w, "Nothing")
	}
}

func (fc *FileControllerImpl) Error(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (fc *FileControllerImpl) Output(w http.ResponseWriter,
	result models.Response) {
	js, err := json.Marshal(result)
	if err != nil {
		fc.Error(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(js)
}
