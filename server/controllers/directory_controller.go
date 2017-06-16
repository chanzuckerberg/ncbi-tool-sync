package controllers

import (
    "github.com/gorilla/mux"
    "net/http"
    "io"
    "ncbi_proj/server/models"
    "ncbi_proj/server/utils"
)

type DirectoryControllerImpl struct {
    ctx *utils.Context
}

func NewDirectoryController(ctx *utils.Context) *DirectoryControllerImpl {
    return &DirectoryControllerImpl{
        ctx: ctx,
    }
}

func (dc *DirectoryControllerImpl) Register(router *mux.Router) {
    router.HandleFunc("/directory", dc.Show)
}

func (dc *DirectoryControllerImpl) Show(w http.ResponseWriter, r *http.Request) {
    dir := new(models.Directory)

    pathName := r.URL.Query().Get("path-name")
    io.WriteString(w, "Path: " + pathName)

    dir.GetLatest(pathName)
}

func (dc *DirectoryControllerImpl) ShowLatest(pathName string) {

}