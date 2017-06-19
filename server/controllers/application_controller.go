package controllers

import (
	"encoding/json"
	"ncbi_proj/server/utils"
	"net/http"
)

type ApplicationController struct {
	ctx *utils.Context
}

func (ac *ApplicationController) Error(w http.ResponseWriter, err error) {
	http.Error(w, "ERROR: "+err.Error(), http.StatusInternalServerError)
}

func (ac *ApplicationController) BadRequest(w http.ResponseWriter) {
	http.Error(w, "Invalid request.", http.StatusBadRequest)
}

func (ac *ApplicationController) Output(w http.ResponseWriter,
	result interface{}) {
	js, err := json.Marshal(result)
	if err != nil {
		ac.Error(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(js)
}
