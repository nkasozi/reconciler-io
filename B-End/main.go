package main

import (
	"fmt"
	"reconciler.io/handlers"
	"reconciler.io/repositories"
	"reconciler.io/servers/http"
)

// @title Reconciliation Service API
// @version 1.0
// @description This is the API for the reconciliation service.
func main() {
	fileDetailsRepo := repositories.NewFileDetailsRepository()
	taskDetailsRepo := repositories.NewTaskDetailsRepository()

	//set up a gin server
	server := http.NewRestApiServer()
	httpRequestHandler := handlers.NewHttpRequestHandler(taskDetailsRepo, fileDetailsRepo)

	//register the routes and handlers
	server.POST("/tasks", httpRequestHandler.CreateReconciliationTask)
	server.POST("/tasks/:id/primary-file", httpRequestHandler.UploadPrimaryFile)
	server.POST("/tasks/:id/comparison-file", httpRequestHandler.UploadComparisonFile)
	server.POST("/tasks/:id/start-reconciliation", httpRequestHandler.StartReconciliation)
	server.GET("/tasks/:id", httpRequestHandler.GetReconciliationTaskStatus)

	// Start the server
	err := server.Run(":9090")

	//error on server start
	if err != nil {
		fmt.Printf("unable to run server: %s", err.Error())
		return
	}
}
