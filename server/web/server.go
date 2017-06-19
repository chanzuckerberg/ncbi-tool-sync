package main

import (
	"database/sql"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"ncbi_proj/server/controllers"
	"ncbi_proj/server/utils"
	"net/http"
	"os"
)

func init() {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// Setup
	ctx := utils.NewContext()
	var err error
	ctx.Db, err = sql.Open("sqlite3", "../versionDB.db")
	defer ctx.Db.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Start server
	router := mux.NewRouter()
	fileController := controllers.NewFileController(ctx)
	fileController.Register(router)
	directoryController := controllers.NewDirectoryController(ctx)
	directoryController.Register(router)

	fmt.Println("STARTING LISTENER")
	err = http.ListenAndServe(":8000", router)

	fmt.Println("ERROR")
	fmt.Println(err)
}