package controllers

import (
	"encoding/json"
	"ncbi_proj/server/utils"
	"net/http"
)

type ApplicationController struct {
	ctx *utils.Context
}

func NewApplicationController(
	ctx *utils.Context) *ApplicationController {
	return &ApplicationController{ctx: ctx}
}

func (ac *ApplicationController) InternalError(w http.ResponseWriter,
	err error) {
	http.Error(w, "Error: "+err.Error(), http.StatusInternalServerError)
}

func (ac *ApplicationController) BadRequest(w http.ResponseWriter) {
	http.Error(w, "Invalid request.", http.StatusBadRequest)
}

func (ac *ApplicationController) Output(w http.ResponseWriter,
	result interface{}) {
	js, err := json.Marshal(result)
	if err != nil {
		ac.InternalError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(js)
}
