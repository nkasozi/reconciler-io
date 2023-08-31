package pre_processing

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"reconciler.io/constants"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/file_storage_locations"
	"reconciler.io/models/enums/supported_file_extensions"
	"reconciler.io/utils"
)

func UploadFile(ctx context.Context, taskDetail models.ReconTaskDetails, filePurpose file_purpose.FilePurposeType, fileDetails *multipart.FileHeader) (*models.FileToBeRead, error) {
	// Generate a unique pre-processing ID based on the hash
	//of the pre-processing content.
	fileID, err := generateFileID(fileDetails)

	if err != nil {
		return nil, err
	}

	fileID = fmt.Sprintf("%v-%v", string(filePurpose), fileID)

	// Determine the pre-processing storage location.
	storageLocation := file_storage_locations.LocalFileSystem // or S3FileSystem based on your setup.

	// Determine for file extension
	fileExtension, err := determineFileExtension(fileDetails)

	if err != nil {
		return nil, err
	}

	// Determine the pre-processing path.
	filePath := utils.GenerateFilePath(fileID, storageLocation, taskDetail.UserID, fileExtension)

	// Check for file metadata
	if err != nil {
		return nil, err
	}

	// Save the pre-processing to the storage location.
	err = saveFile(fileDetails, filePath)

	if err != nil {
		return nil, err
	}

	readResultsStream, err := createStream(ctx, fileID, filePurpose)

	if err != nil {
		return nil, err
	}
	// Create the FileToBeRead object.
	fileToBeRead := models.FileToBeRead{
		ID:                   fileID,
		ReconciliationTaskID: taskDetail.ID,
		FilePurpose:          filePurpose,
		FileMetadata: models.FileMetadata{
			HasHeaderRow:     false,
			ColumnDelimiters: []rune{','},
		},
		FileStorageLocation:   storageLocation,
		FileExtension:         fileExtension,
		FilePath:              filePath,
		ReadFileResultsStream: readResultsStream,
	}

	return &fileToBeRead, nil
}

func createStream(ctx context.Context, fileId string, filePurpose file_purpose.FilePurposeType) (models.StreamProvider, error) {
	streamProvider, err := models.NewStreamProvider(constants.NATS_URL)
	topicName := fileId
	switch filePurpose {
	case file_purpose.PrimaryFile:
		err = streamProvider.SetupStream(
			ctx,
			constants.PRIMARY_FILE_SECTIONS_STREAM_NAME,
			topicName,
		)
		if err != nil {
			err = fmt.Errorf("error on creating PrimaryFileSectionsStream: [%v]", err)
			return nil, err
		}
	case file_purpose.ComparisonFile:
		err = streamProvider.SetupStream(ctx,
			constants.COMPARISON_FILE_SECTIONS_STREAM_NAME,
			topicName,
		)
		if err != nil {
			err = fmt.Errorf("error on creating ComparisonFileSectionsStream: [%v]", err)
			return nil, err
		}
	}

	return streamProvider, nil
}

func determineFileExtension(fileDetails *multipart.FileHeader) (supported_file_extensions.FileExtension, error) {
	extension := filepath.Ext(fileDetails.Filename)
	switch extension {
	case ".csv":
		return supported_file_extensions.Csv, nil
	default:
		return "", errors.New("unsupported file extension")
	}
}

func generateFileID(file *multipart.FileHeader) (string, error) {
	// Open the file
	src, err := file.Open()
	if err != nil {
		return "", err
	}
	defer src.Close()

	// Create a new hash
	h := sha256.New()
	if _, err := io.Copy(h, src); err != nil {
		return "", err
	}

	// Return the hash as a hex string
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func saveFile(file *multipart.FileHeader, filePath string) error {
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, src)
	return err
}
