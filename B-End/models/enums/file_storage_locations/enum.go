package file_storage_locations

type FileStorageLocation string

const (
	LocalFileSystem FileStorageLocation = "LocalFileSystem"
	S3FileSystem    FileStorageLocation = "S3FileSystem"
)
