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

	// Apply the repository middleware to the router
	server.Use(repositories.FileDetailsRepositoryMiddleware(fileDetailsRepo))
	server.Use(repositories.TaskDetailsRepositoryMiddleware(taskDetailsRepo))

	//register the routes and handlers
	server.POST("/tasks", handlers.CreateReconciliationTask)
	server.POST("/tasks/:id/primary-file", handlers.UploadPrimaryFile)
	server.POST("/tasks/:id/comparison-file", handlers.UploadComparisonFile)
	server.POST("/tasks/:id/start-reconciliation", handlers.StartReconciliation)
	server.GET("/tasks/:id", handlers.GetReconciliationTaskStatus)

	// Start the server
	err := server.Run(":9090")

	//error on server start
	if err != nil {
		fmt.Printf("unable to run server: %s", err.Error())
		return
	}
}
