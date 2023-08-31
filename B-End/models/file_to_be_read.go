package models

import (
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/file_storage_locations"
	"reconciler.io/models/enums/supported_file_extensions"
)

type FileToBeRead struct {
	ID                    string
	ReconciliationTaskID  string
	FilePurpose           file_purpose.FilePurposeType
	ColumnHeaders         []string
	FileMetadata          FileMetadata
	FileStorageLocation   file_storage_locations.FileStorageLocation
	FileExtension         supported_file_extensions.FileExtension
	FilePath              string         `json:"-"`
	ReadFileResultsStream StreamProvider `json:"-"`
}

type FileMetadata struct {
	HasHeaderRow     bool
	ColumnDelimiters []rune
}
