package controllers

import (
    //"ncbi_proj/server/models"
    "github.com/gorilla/mux"
    "net/http"
    "io"
    "strconv"
    "database/sql"
    "ncbi_proj/server/models"
)

type FileControllerImpl struct {
    db  *sql.DB
}

func NewFileController(db *sql.DB) *FileControllerImpl {
    return &FileControllerImpl{
        db: db,
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

    if pathName != "" && versionNum != "" {
        // Serve up that version of the file
        io.WriteString(w, "\n" + pathName)
        io.WriteString(w, "\n" + versionNum)
        num, _ := strconv.Atoi(versionNum)
        file.Get(pathName, num, fc.db)
    } else if pathName != "" && versionNum == "" {
        if op == "history" {
            // Serve up file history
            file.GetHistory(pathName)
        } else {
            // Serve up the latest version of the file
            file.GetLatest(pathName)
        }
    } else {
        //return errors.New("No name or version number")
        io.WriteString(w, "Nothing")
    }
}

func (fc *FileControllerImpl) ShowLatest(pathName string) {

}

func (fc *FileControllerImpl) ShowHistory(pathName string) {

}