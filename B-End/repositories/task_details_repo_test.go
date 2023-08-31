package repositories

import (
	"context"
	"reconciler.io/models"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateReconciliationTask(t *testing.T) {
	ctx := context.Background()
	taskDetails := models.ReconTaskDetails{
		IsDone:          false,
		HasBegun:        false,
		ComparisonPairs: []models.ComparisonPair{},
		ReconConfig:     models.ReconciliationConfigs{},
	}

	taskID, err := SaveTaskDetails(ctx, taskDetails)
	assert.NoError(t, err)
	assert.NotEmpty(t, taskID)
}

func TestUpdateReconciliationTask(t *testing.T) {
	ctx := context.Background()
	taskDetails := models.ReconTaskDetails{
		IsDone:          false,
		HasBegun:        false,
		ComparisonPairs: []models.ComparisonPair{},
		ReconConfig:     models.ReconciliationConfigs{},
	}

	// Create a task first.
	taskID, _ := SaveTaskDetails(ctx, taskDetails)

	// Update the task.
	taskDetails.IsDone = true
	err := UpdateReconciliationTask(ctx, taskID, taskDetails)
	assert.NoError(t, err)

	// Retrieve the task and check the updated value.
	updatedTask, _ := GetReconciliationTaskStatus(ctx, taskID)
	assert.True(t, updatedTask.IsDone)
}

func TestGetReconciliationTaskStatus(t *testing.T) {
	ctx := context.Background()
	taskDetails := models.ReconTaskDetails{
		IsDone:          false,
		HasBegun:        false,
		ComparisonPairs: []models.ComparisonPair{},
		ReconConfig:     models.ReconciliationConfigs{},
	}

	// Create a task first.
	taskID, _ := SaveTaskDetails(ctx, taskDetails)

	// Retrieve the task.
	retrievedTask, err := GetReconciliationTaskStatus(ctx, taskID)
	assert.NoError(t, err)
	assert.Equal(t, taskID, retrievedTask.ID)
}

func TestGetReconciliationTaskStatus_NotFound(t *testing.T) {
	ctx := context.Background()

	// Try to retrieve a non-existent task.
	_, err := GetReconciliationTaskStatus(ctx, "non_existent_task")
	assert.Error(t, err)
}
