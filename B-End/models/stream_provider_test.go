package models

import (
	"context"
	"github.com/stretchr/testify/assert"
	"reconciler.io/models/enums/file_purpose"
	"testing"
	"time"
)

func TestNatsStreamProviderIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Initialize StreamProvider
	streamProvider, err := NewStreamProvider("nats://localhost:4222")
	assert.NoError(t, err)

	// Setup Stream
	err = streamProvider.SetupStream(ctx, "testStream", "testTopic.*")
	assert.NoError(t, err)

	// Publish to Topic
	msg := FileSection{
		ID:                    "1234",
		TaskID:                "1234",
		FileID:                "random-file-id",
		SectionSequenceNumber: 1,
		OriginalFilePurpose:   file_purpose.PrimaryFile,
		SectionRows:           nil,
		ComparisonPairs:       nil,
		ColumnHeaders:         nil,
		ReconConfig:           ReconciliationConfigs{},
		IsLastSection:         false,
	}
	err = streamProvider.PublishToTopic(
		ctx,
		"testTopic.2",
		msg,
	)
	assert.NoError(t, err)

	// Create Consumer
	sectionConsumer, err := streamProvider.CreateStreamConsumer(
		ctx,
		"testStream",
		"testTopic2",
		"testConsumer2",
	)
	assert.NoError(t, err)

	// Fetch Next Section
	fileSection, err := sectionConsumer.FetchNext()
	assert.NoError(t, err)

	expectedFileSection := msg
	assert.Equal(t, expectedFileSection, *fileSection)
}
