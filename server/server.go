package main

import (
	"database/sql"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"log"
	_ "github.com/go-sql-driver/mysql"
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
	ctx.Db, err = sql.Open("mysql",
		"tool:MrnUaj6Epq2@/versions")
	defer ctx.Db.Close()
	if err != nil {
		log.Fatal("Failed to open database.")
		log.Fatal(err)
	}
	err = ctx.Db.Ping()
	if err != nil {
		log.Fatal("Failed to ping database.")
		log.Fatal(err)
	}

	// Routing
	router := mux.NewRouter()
	fileController := controllers.NewFileController(ctx)
	fileController.Register(router)
	directoryController := controllers.NewDirectoryController(ctx)
	directoryController.Register(router)
	router.NotFoundHandler = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "Page not found.")
		})

	// Start server
	log.Println("Starting listener...")
	err = http.ListenAndServe(":8000", router)
	if err != nil {
		log.Fatal("Error in running listen and serve.")
	}
}
