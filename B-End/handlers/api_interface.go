package handlers

import (
	"github.com/gin-gonic/gin"
	"reconciler.io/models"
	"reconciler.io/repositories"
)

type ApiRequestHandler interface {
	StartReconciliation(ctx *gin.Context)
	CreateReconciliationTask(ctx *gin.Context)
	GetReconciliationTaskStatus(c *gin.Context)
	UploadPrimaryFile(ctx *gin.Context)
	BeginFileReconstructionProcesses(taskInfo models.ReconTaskDetails)
	BeginFileReconciliationProcesses(
		taskInfo models.ReconTaskDetails,
		taskDetailsRepo *repositories.TaskDetailsRepository,
		fileDetailsRepo *repositories.FileDetailsRepository,
	)
	UploadComparisonFile(ctx *gin.Context)
}
