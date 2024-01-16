package repositories

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"sync"

	"reconciler.io/models"
	"strconv"
)

type TaskDetailsRepository struct {
	reconTasksMap   map[string]*models.ReconTaskDetails
	reconTasksMutex sync.Mutex
}

func NewTaskDetailsRepository() *TaskDetailsRepository {
	return &TaskDetailsRepository{
		reconTasksMap: make(map[string]*models.ReconTaskDetails),
	}
}

func TaskDetailsRepositoryMiddleware(repo *TaskDetailsRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("TaskDetailsRepository", repo)
		c.Next()
	}
}

func (r *TaskDetailsRepository) SaveTaskDetails(ctx context.Context, taskDetails models.ReconTaskDetails) (string, error) {
	r.reconTasksMutex.Lock()
	defer r.reconTasksMutex.Unlock()

	if len(taskDetails.ID) <= 0 {
		nextID := len(r.reconTasksMap) + 1
		taskID := "task_" + strconv.Itoa(nextID)
		taskDetails.ID = taskID
	}

	r.reconTasksMap[taskDetails.ID] = &taskDetails

	return taskDetails.ID, nil
}

func (r *TaskDetailsRepository) UpdateReconciliationTask(ctx context.Context, taskDetails models.ReconTaskDetails) error {
	r.reconTasksMutex.Lock()
	defer r.reconTasksMutex.Unlock()

	if _, exists := r.reconTasksMap[taskDetails.ID]; !exists {
		return errors.New("task not found")
	}

	r.reconTasksMap[taskDetails.ID] = &taskDetails

	return nil
}

func (r *TaskDetailsRepository) AttachPrimaryFile(ctx context.Context, taskID, primaryFileID string) error {
	r.reconTasksMutex.Lock()
	defer r.reconTasksMutex.Unlock()

	if _, exists := r.reconTasksMap[taskID]; !exists {
		return errors.New("task not found")
	}

	r.reconTasksMap[taskID].PrimaryFileID = primaryFileID

	return nil
}

func (r *TaskDetailsRepository) AttachComparisonFile(ctx context.Context, taskID, comparisonFileID string) error {
	r.reconTasksMutex.Lock()
	defer r.reconTasksMutex.Unlock()

	if _, exists := r.reconTasksMap[taskID]; !exists {
		return errors.New("task not found")
	}

	r.reconTasksMap[taskID].ComparisonFileID = comparisonFileID

	return nil
}

func (r *TaskDetailsRepository) GetReconciliationTaskStatus(ctx context.Context, taskID string) (models.ReconTaskDetails, error) {
	r.reconTasksMutex.Lock()
	defer r.reconTasksMutex.Unlock()

	task, exists := r.reconTasksMap[taskID]
	if !exists {
		errorDetail := fmt.Sprintf("task with ID [%v] not found", taskID)
		return models.ReconTaskDetails{}, errors.New(errorDetail)
	}

	return *task, nil
}
