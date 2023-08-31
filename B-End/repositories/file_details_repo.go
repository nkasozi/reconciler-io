package repositories

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"sync"
)

type FileDetailsRepository struct {
	fileDetailsMap   map[string]*models.FileToBeRead
	fileDetailsMutex sync.Mutex
}

func NewFileDetailsRepository() *FileDetailsRepository {
	return &FileDetailsRepository{
		fileDetailsMap: make(map[string]*models.FileToBeRead),
	}
}

func FileDetailsRepositoryMiddleware(repo *FileDetailsRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("FileDetailsRepository", repo)
		c.Next()
	}
}

func (m *FileDetailsRepository) SaveFileToBeRead(ctx context.Context, fileToBeRead models.FileToBeRead) (string, error) {
	m.fileDetailsMutex.Lock()
	defer m.fileDetailsMutex.Unlock()

	if len(fileToBeRead.ID) <= 0 {
		fileId := "file_" + string(rune(len(m.fileDetailsMap)+1))
		fileToBeRead.ID = fileId
	}

	m.fileDetailsMap[fileToBeRead.ID] = &fileToBeRead

	return fileToBeRead.ID, nil
}

func (m *FileDetailsRepository) UpdateFileDetails(ctx context.Context, fileToBeRead models.FileToBeRead) error {
	m.fileDetailsMutex.Lock()
	defer m.fileDetailsMutex.Unlock()

	if _, exists := m.fileDetailsMap[fileToBeRead.ID]; !exists {
		return errors.New("task not found")
	}

	m.fileDetailsMap[fileToBeRead.ID] = &fileToBeRead

	return nil
}

func (m *FileDetailsRepository) GetFileDetails(ctx context.Context, Id string) (*models.FileToBeRead, error) {
	m.fileDetailsMutex.Lock()
	defer m.fileDetailsMutex.Unlock()

	file, exists := m.fileDetailsMap[Id]
	if !exists {
		return nil, errors.New("task not found")
	}

	return file, nil
}

func (m *FileDetailsRepository) GetPrimaryFileDetailsForTask(ctx context.Context, TaskId string) (models.FileToBeRead, error) {
	m.fileDetailsMutex.Lock()
	defer m.fileDetailsMutex.Unlock()

	for _, file := range m.fileDetailsMap {
		if file.ReconciliationTaskID == TaskId &&
			file.FilePurpose == file_purpose.PrimaryFile {
			return *file, nil
		}
	}
	return models.FileToBeRead{}, errors.New("primaryFile for Task not found")
}

func (m *FileDetailsRepository) GetComparisonFileDetailsForTask(ctx context.Context, TaskId string) (models.FileToBeRead, error) {
	m.fileDetailsMutex.Lock()
	defer m.fileDetailsMutex.Unlock()

	for _, file := range m.fileDetailsMap {
		if file.ReconciliationTaskID == TaskId &&
			file.FilePurpose == file_purpose.ComparisonFile {
			return *file, nil
		}
	}
	return models.FileToBeRead{}, errors.New("comparisonFile for Task not found")
}
