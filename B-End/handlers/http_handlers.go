package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	preprocessing "reconciler.io/activities/pre-processing"
	"reconciler.io/activities/reconciliation"
	"reconciler.io/activities/reconstruction"
	"reconciler.io/constants"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/file_storage_locations"
	"reconciler.io/models/enums/supported_file_extensions"
	"reconciler.io/repositories"
	"reconciler.io/utils"
)

type HttpRequestHandler struct {
	taskDetailsRepository *repositories.TaskDetailsRepository
	fileDetailsRepository *repositories.FileDetailsRepository
}

func NewHttpRequestHandler(
	taskDetailsRepository *repositories.TaskDetailsRepository,
	fileDetailsRepository *repositories.FileDetailsRepository,
) ApiRequestHandler {
	return &HttpRequestHandler{
		taskDetailsRepository: taskDetailsRepository,
		fileDetailsRepository: fileDetailsRepository,
	}
}

func (h HttpRequestHandler) StartReconciliation(ctx *gin.Context) {
	//get the taskID for the recon status
	taskID := ctx.Param("id")

	//no taskID found
	if len(taskID) <= 0 {
		errorDetail := fmt.Sprintf("task with ID [%v] not found", taskID)
		ctx.JSON(400, gin.H{"error": errorDetail})
		return
	}

	// retrieve the details for the original task
	taskDetails, err := h.taskDetailsRepository.GetReconciliationTaskStatus(ctx, taskID)

	// error on retrieve
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	go h.BeginFileReconciliationProcesses(taskDetails, h.taskDetailsRepository, h.fileDetailsRepository)

	ctx.JSON(201, gin.H{"ReconStartedForTaskID": taskID})
}

// CreateReconciliationTask
// @Summary Create a reconciliation task
// @Accept  json
// @Produce  json
// @Param   body models.ReconTaskDetails true "Reconciliation Task Details"
// @Success 201 {object} map[string]string
// @Failure 400 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router  /tasks [post]
func (h HttpRequestHandler) CreateReconciliationTask(ctx *gin.Context) {

	parsed, err := utils.ParseAndBindJsonToStruct(ctx, &models.ReconTaskDetails{})
	if err != nil {
		ctx.JSON(400, gin.H{"error": "Failed parse json model", "details": err.Error()})
		return
	}

	var taskDetails = parsed.(*models.ReconTaskDetails)

	err = utils.ValidateStruct(ctx, taskDetails)
	if err != nil {
		ctx.JSON(400, gin.H{"error": "Validation Failure", "details": err.Error()})
		return
	}

	toBeReconstructedFileSectionsStream, err := models.NewStreamProvider(constants.NATS_URL)

	if err != nil {
		err = fmt.Errorf("error on creating toBeReconstructedFileSectionsStream: [%v]", err)
		ctx.JSON(500, gin.H{"error": "InternalServerError", "details": err.Error()})
	}

	topicName := fmt.Sprintf("Reconstruct-%v", taskDetails.ID)
	err = toBeReconstructedFileSectionsStream.SetupStream(
		ctx,
		constants.FILE_RECONSTRUCTION_STREAM_NAME,
		topicName,
	)

	if err != nil {
		err = fmt.Errorf("error on setting up toBeReconstructedFileSectionsStream: [%v]", err)
		ctx.JSON(500, gin.H{"error": "InternalServerError", "details": err.Error()})
		return
	}

	taskDetails.FileToBeReconstructedChannel = toBeReconstructedFileSectionsStream

	taskID, err := h.taskDetailsRepository.SaveTaskDetails(ctx, *taskDetails)

	if err != nil {
		ctx.JSON(500, gin.H{"error": "InternalServerError", "details": err.Error()})
		return
	}

	ctx.JSON(201, gin.H{"taskID": taskID})
}

// @Summary Get the status of a reconciliation task
// @Produce  json
// @Param   id path string true "Task ID"
// @Success 200 {object} models.ReconTaskDetails
// @Failure 400 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router  /tasks/{id} [get]

func (h HttpRequestHandler) GetReconciliationTaskStatus(c *gin.Context) {
	// Retrieve the task status
	taskId := c.Param("id")
	status, err := h.taskDetailsRepository.GetReconciliationTaskStatus(context.Background(), taskId)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, status)
}

func (h HttpRequestHandler) UploadPrimaryFile(ctx *gin.Context) {
	//get the taskID for the recon status
	taskID := ctx.Param("id")

	//no taskID found
	if len(taskID) <= 0 {
		errorDetail := fmt.Sprintf("task with ID [%v] not found", taskID)
		ctx.JSON(400, gin.H{"error": errorDetail})
		return
	}

	//access the uploaded file details
	fileInfo, err := ctx.FormFile("file")

	//error on access
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// retrieve the details for the original task
	taskDetails, err := h.taskDetailsRepository.GetReconciliationTaskStatus(ctx, taskID)

	// error on retrieve
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// upload the file for pre-processing
	fileToBeRead, err := preprocessing.UploadFile(ctx, taskDetails, file_purpose.PrimaryFile, fileInfo)

	// error on upload
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// save the fileToBeRead details
	_, err = h.fileDetailsRepository.SaveFileToBeRead(ctx, *fileToBeRead)

	// error on save
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// update task details the fileToBeRead details
	err = h.taskDetailsRepository.AttachPrimaryFile(ctx, taskDetails.ID, fileToBeRead.ID)

	// error on save
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	//start reading the file asynchronously
	go BeginFileReadingProcesses(*fileToBeRead, taskDetails)

	//start file reconciliation processes
	//go BeginFileReconciliationProcesses(taskDetails, taskDetailsRepository, fileDetailsRepository)

	//return success
	ctx.JSON(200, fileToBeRead)
}

func BeginFileReadingProcesses(fileToRead models.FileToBeRead, taskInfo models.ReconTaskDetails) {
	sectionSize := constants.FILE_SECTION_BATCH_SIZE
	err := preprocessing.ReadFileIntoChannel(context.Background(), fileToRead, taskInfo, sectionSize)
	if err != nil {
		log.Fatalf("Error on reading PrimaryFile: %s", err.Error())
	}
}

func (h HttpRequestHandler) BeginFileReconstructionProcesses(taskInfo models.ReconTaskDetails) {
	// Recovery mechanism
	defer func() {
		if r := recover(); r != nil {
			log.Printf("BeginFileReconstructionProcesses goroutine panicked with error: %v", r)
		}
	}()
	prefix := fmt.Sprintf("ReconResults-%v", taskInfo.ID)
	filePath := utils.GenerateFilePath(prefix, file_storage_locations.LocalFileSystem, taskInfo.UserID, supported_file_extensions.Csv)

	err := reconstruction.ReconstructFile(taskInfo.ID, taskInfo.FileToBeReconstructedChannel, filePath)
	if err != nil {
		log.Printf("Error on File Reconstruction: [%v]", err.Error())
	}
}

func (h HttpRequestHandler) BeginFileReconciliationProcesses(
	taskInfo models.ReconTaskDetails,
	taskDetailsRepo *repositories.TaskDetailsRepository,
	fileDetailsRepo *repositories.FileDetailsRepository,
) {
	// Recovery mechanism
	defer func() {
		if r := recover(); r != nil {
			log.Printf("BeginFileReconciliationProcesses goroutine panicked with error: %v", r)
		}
	}()
	//see if we have already begun reconciliation for this
	if taskInfo.HasBegun {
		log.Printf("Reconcialition already began for taskID [%s]", taskInfo.ID)
		return
	}

	//if the fileToRead passed in is a comparisonFile,
	//we need to find the matching primaryFile and vice versa
	primaryFile, comparisonFile, err := h.determinePrimaryAndComparisonFiles(taskInfo.ID, fileDetailsRepo)

	//error on determining
	if err != nil {
		log.Printf(err.Error())
		return
	}

	//update the original recon tasks status
	taskInfo.HasBegun = true
	err = taskDetailsRepo.UpdateReconciliationTask(context.Background(), taskInfo)
	if err != nil {
		log.Printf("Error on updating recon task details: %s", err.Error())
		return
	}

	//if it exists, then we can begin file reconciliation processes
	//first we spawn a handler for the file reconstruction
	go h.BeginFileReconstructionProcesses(taskInfo)

	//now we can start the reconciliation
	err = reconciliation.BeginFileReconciliation(primaryFile, comparisonFile, taskInfo)

	//error on reconciliation
	if err != nil {
		log.Printf("Error on Reconciliation Start: %s", err.Error())
		return
	}
}

func (h HttpRequestHandler) determinePrimaryAndComparisonFiles(taskID string, fileDetailsRepo *repositories.FileDetailsRepository) (primaryFile models.FileToBeRead, comparisonFile models.FileToBeRead, err error) {
	//if the fileToRead passed in is a comparisonFile,
	//we need to find the matching primaryFile
	comparisonFile, err = fileDetailsRepo.GetComparisonFileDetailsForTask(context.Background(), taskID)

	// if it's not found
	if err != nil {
		errorDetails := fmt.Sprintf("Error on retrieving ComparisonFile: %s", err.Error())
		return models.FileToBeRead{}, models.FileToBeRead{}, errors.New(errorDetails)
	}

	primaryFile, err = fileDetailsRepo.GetPrimaryFileDetailsForTask(context.Background(), taskID)

	// if it's not found
	if err != nil {
		errorDetails := fmt.Sprintf("Error on retrieving PrimaryFile: %s", err.Error())
		return models.FileToBeRead{}, models.FileToBeRead{}, errors.New(errorDetails)
	}
	return primaryFile, comparisonFile, nil

}

func (h HttpRequestHandler) UploadComparisonFile(ctx *gin.Context) {
	taskDetailsRepository := ctx.MustGet("TaskDetailsRepository").(*repositories.TaskDetailsRepository)
	fileDetailsRepository := ctx.MustGet("FileDetailsRepository").(*repositories.FileDetailsRepository)

	//get the taskID for the recon status
	taskID := ctx.Param("id")

	//no taskID found
	if len(taskID) <= 0 {
		errorDetail := fmt.Sprintf("task with ID [%v] not found", taskID)
		ctx.JSON(400, gin.H{"error": errorDetail})
		return
	}

	//access the uploaded file details
	fileInfo, err := ctx.FormFile("file")

	//error on access
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// retrieve the details for the original task
	taskDetails, err := taskDetailsRepository.GetReconciliationTaskStatus(ctx, taskID)

	// error on retrieve
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// upload the file for pre-processing
	fileToBeRead, err := preprocessing.UploadFile(ctx, taskDetails, file_purpose.ComparisonFile, fileInfo)

	// error on upload
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// save the fileToBeRead details
	_, err = fileDetailsRepository.SaveFileToBeRead(ctx, *fileToBeRead)

	// error on save
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	err = taskDetailsRepository.AttachComparisonFile(ctx, taskDetails.ID, fileToBeRead.ID)

	// error on save
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	//start reading the file asynchronously
	go BeginFileReadingProcesses(*fileToBeRead, taskDetails)

	//return success
	ctx.JSON(200, fileToBeRead)
}
