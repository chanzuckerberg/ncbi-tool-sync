package server

import (
	"database/sql"
	"fmt"
	"github.com/gorilla/mux"
	"ncbi_proj/server/controllers"
	"net/http"
	_ "github.com/mattn/go-sqlite3"
	"ncbi_proj/server/utils"
)

func Main() error {
	// Setup
	ctx := utils.NewContext()
	var err error
	ctx.Db, err = sql.Open("sqlite3", "./versionDB.db")
	defer ctx.Db.Close()
	if err != nil {
		return err
	}

	// Start server
	router := mux.NewRouter()
	fileController := controllers.NewFileController(ctx)
	fileController.Register(router)
	directoryController := controllers.NewDirectoryController(ctx)
	directoryController.Register(router)

	fmt.Println("STARTING LISTENER")
	err = http.ListenAndServe(":8000", router)

	return err
}