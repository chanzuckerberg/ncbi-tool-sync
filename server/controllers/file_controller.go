package controllers

import (
    "github.com/gorilla/mux"
    "net/http"
    "io"
    "strconv"
    "ncbi_proj/server/models"
    "ncbi_proj/server/utils"
    "encoding/json"
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

    if pathName != "" && versionNum != "" {
        // Serve up that version of the file
        num, _ := strconv.Atoi(versionNum)
        result, err := file.Get(pathName, num, fc.ctx)
        js, err := json.Marshal(result)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        fc.WriteWithHeader(w, js)
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

func (fc *FileControllerImpl) WriteWithHeader(w http.ResponseWriter, res []byte) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write(res)
}

func (fc *FileControllerImpl) ShowLatest(pathName string) {

}

func (fc *FileControllerImpl) ShowHistory(pathName string) {

}