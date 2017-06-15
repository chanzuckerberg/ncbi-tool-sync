package server

import (
    "net/http"
    "io"
    "fmt"
)

func Main() error {
    mux := http.NewServeMux()
    mux.HandleFunc("/file", fileHandler)
    fmt.Println("STARTING LISTENER")
    err := http.ListenAndServe(":8000", mux)
    return err
}

func fileHandler(w http.ResponseWriter, r *http.Request) {
    pathName := r.URL.Query().Get("path-name")
    versionNum := r.URL.Query().Get("version-num")
    op := r.URL.Query().Get("op")

    if op == "history" {
        // Serve up version history

    } else {
        if pathName != "" && versionNum != "" {
            // Serve up that version of the file
            io.WriteString(w, "\n" + pathName)
            io.WriteString(w, "\n" + versionNum)
        } else if versionNum != "" {
            io.WriteString(w, versionNum)
            // Serve up latest version of the file
        } else {
            //return errors.New("No name or version number")
            io.WriteString(w, "Nothing")
        }
    }
}