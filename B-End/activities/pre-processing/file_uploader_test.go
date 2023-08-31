package pre_processing

import (
	"bytes"
	"context"
	"reconciler.io/models"
	"testing"
)

func TestUploadFile(t *testing.T) {
	ctx := context.Background()
	taskID := "test_task_id"
	filePurpose := models.PrimaryFile
	fileContent := bytes.NewReader([]byte("pre-processing content"))
	fileExtension := models.Csv
	metadata := models.FileMetadata{
		ColumnDelimiters: []rune{','},
	}

	// Test successful pre-processing upload.
	fileToBeRead, err := UploadFile(ctx, taskID, filePurpose, fileContent, fileExtension, metadata)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if fileToBeRead.ID == "" {
		t.Errorf("Expected pre-processing ID to be generated, got empty string")
	}

	// Test invalid task ID.
	_, err = UploadFile(ctx, "", filePurpose, fileContent, fileExtension, metadata)
	if err == nil {
		t.Errorf("Expected error for invalid task ID, got nil")
	}
}

// TODO: Add more test cases for different scenarios, such as pre-processing saving failure, pre-processing ID generation failure, etc.
