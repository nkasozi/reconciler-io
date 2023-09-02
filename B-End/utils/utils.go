package utils

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"reconciler.io/constants"
	"reconciler.io/models/enums/file_storage_locations"
	"reconciler.io/models/enums/supported_file_extensions"
	"time"
)

func GenerateFilePath(
	fileID string,
	storageLocation file_storage_locations.FileStorageLocation,
	userId string,
	fileExtension supported_file_extensions.FileExtension,
) string {
	timestamp := time.Now().Format("20060102150405") // YYYYMMDDHHMMSS format
	return fmt.Sprintf("./%s_%s.%s", fileID, timestamp, fileExtension)
}

// ParseAndBindJsonToStruct Parse and bind activity
func ParseAndBindJsonToStruct(c *gin.Context, outStruct interface{}) (interface{}, error) {
	if err := c.ShouldBindJSON(outStruct); err != nil {
		return nil, err
	}
	return outStruct, nil
}

// ValidateStruct Generic validation activity using go-validator
func ValidateStruct(_ context.Context, inStruct interface{}) error {
	validate := validator.New()
	err := validate.Struct(inStruct)
	if err != nil {
		return err
	}
	return nil
}

// NewContextWithTimeout creates a new context with a specific timeout duration.
func NewContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// NewContextWithDefaultTimeout creates a new context with a specific timeout duration.
func NewContextWithDefaultTimeout() context.Context {
	ctx, _ := NewContextWithTimeout(constants.DEFAULT_NATS_TIMEOUT_IN_MINUTES)
	return ctx
}
