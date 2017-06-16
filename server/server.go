package server

import (
    "net/http"
    "io"
    "fmt"
    "strconv"
    "database/sql"
    "ncbi_proj/server/controllers"
    "github.com/gorilla/mux"
)

type Context struct {
    db         *sql.DB
}

func Main() error {
    // Load db
    var err error
    var ctx Context
    ctx.db, err = sql.Open("sqlite3", "./versionDB.db")
    defer ctx.db.Close()
    if err != nil {
        return err
    }

    // Start server
    router := mux.NewRouter()
    fileController := controllers.NewFileController(ctx.db)
    fileController.Register(router)
    directoryController := controllers.NewDirectoryController(ctx.db)
    directoryController.Register(router)

    fmt.Println("STARTING LISTENER")
    err = http.ListenAndServe(":8000", mux)

    return err
}
