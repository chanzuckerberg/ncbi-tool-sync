package server

import (
    "net/http"
    "io"
    "fmt"
    "ncbi_proj/server/file"
    "strconv"
    "ncbi_proj/server/directory"
)

func Main() error {
    mux := http.NewServeMux()
    mux.HandleFunc("/file", fileHandler)
    mux.HandleFunc("/directory", directoryHandler)
    fmt.Println("STARTING LISTENER")
    err := http.ListenAndServe(":8000", mux)
    return err
}

func fileHandler(w http.ResponseWriter, r *http.Request) {
    pathName := r.URL.Query().Get("path-name")
    versionNum := r.URL.Query().Get("version-num")
    op := r.URL.Query().Get("op")

    if pathName != "" && versionNum != "" {
        // Serve up that version of the file
        io.WriteString(w, "\n" + pathName)
        io.WriteString(w, "\n" + versionNum)
        num, _ := strconv.Atoi(versionNum)
        file.Show(pathName, num)
    } else if pathName != "" && versionNum == "" {
        if op == "history" {
            // Serve up file history
            file.ShowHistory(pathName)
        } else {
            // Serve up the latest version of the file
            file.ShowLatest(pathName)
        }
    } else {
        //return errors.New("No name or version number")
        io.WriteString(w, "Nothing")
    }

}

func directoryHandler(w http.ResponseWriter, r *http.Request) {
    pathName := r.URL.Query().Get("path-name")
    io.WriteString(w, "Path: " + pathName)

    directory.ShowLatest(pathName)
}
