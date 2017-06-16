package controllers

import (
    "github.com/gorilla/mux"
    "net/http"
    "io"
    "ncbi_proj/server/models"
    "database/sql"
)

type DirectoryControllerImpl struct {
    db  *sql.DB
}

func NewDirectoryController(db *sql.DB) *DirectoryControllerImpl {
    return &DirectoryControllerImpl{
        db: db,
    }
}

func (dc *DirectoryControllerImpl) Register(router *mux.Router) {
    router.HandleFunc("/directory", dc.Show)
}

func (dc *DirectoryControllerImpl) Show(w http.ResponseWriter, r *http.Request) {
    //file := new(models.File)
    //models.get(pathName, versionNum)
    dir := new(models.Directory)

    pathName := r.URL.Query().Get("path-name")
    io.WriteString(w, "Path: " + pathName)

    dir.GetLatest(pathName)
}

func (dc *DirectoryControllerImpl) ShowLatest(pathName string) {

}
