package pre_processing

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"reconciler.io/models"
	"reconciler.io/models/enums/recon_status"
	"reconciler.io/models/enums/supported_file_extensions"
	"strconv"
	"strings"
)

// ReadFileIntoChannel reads a file and converts it into a stream of FileSections.
func ReadFileIntoChannel(ctx context.Context, fileToBeRead models.FileToBeRead, taskDetails models.ReconTaskDetails, sectionSize int) error {
	// Validate the input
	if fileToBeRead.ID == "" || fileToBeRead.FilePath == "" || sectionSize <= 0 {
		return errors.New("invalid file details or section size")
	}

	// Switch based on the file extension
	switch fileToBeRead.FileExtension {
	case supported_file_extensions.Csv:
		return readCSVFile(ctx, fileToBeRead, taskDetails, sectionSize)
	default:
		return errors.New("unsupported file extension")
	}
}

// readCSVFile reads and parses a CSV file.
func readCSVFile(ctx context.Context, fileToBeRead models.FileToBeRead, taskDetails models.ReconTaskDetails, sectionSize int) error {
	// Open the CSV file
	file, err := os.Open(fileToBeRead.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read the CSV file using a CSV reader
	reader := csv.NewReader(file)

	var sectionRows []models.FileSectionRow
	rowNumber := uint64(0)
	sectionSequenceNumber := 1
	columnHeaders := make([]string, 0)
	isFirstTime := true
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if isFirstTime {
			columnHeaders = determineColumnHeaders(fileToBeRead, rowNumber, record)
			isFirstTime = false
		}

		log.Printf("Reading Line [%v]", rowNumber)

		row := models.FileSectionRow{
			RowNumber:            rowNumber,
			RawData:              strconv.Quote(strings.Join(record, ",")),
			ParsedColumnsFromRow: record,
			ReconResult:          recon_status.Pending,
			ReconResultReasons:   []string{},
		}
		sectionRows = append(sectionRows, row)

		if len(sectionRows) == sectionSize {
			log.Printf("BatchSize limit reached Line")
			fileSection := models.FileSection{
				ID:                    uuid.New().String(),
				TaskID:                taskDetails.ID,
				FileID:                fileToBeRead.ID,
				SectionSequenceNumber: sectionSequenceNumber,
				OriginalFilePurpose:   fileToBeRead.FilePurpose,
				SectionRows:           sectionRows,
				ComparisonPairs:       taskDetails.ComparisonPairs,
				ColumnHeaders:         columnHeaders,
				ReconConfig:           taskDetails.ReconConfig,
				IsLastSection:         false,
			}

			err = publishSectionToStream(ctx, fileToBeRead, fileSection)
			if err != nil {
				return err
			}
			sectionRows = sectionRows[:0] // Reset the sectionRows slice
			sectionSequenceNumber++
		}

		rowNumber++
	}

	// Add the last section if there are remaining rows
	log.Printf("Finished Reading File on Record [%v]", rowNumber)
	if len(sectionRows) > 0 {
		log.Printf("Inputing last fileSection for file [%v]", fileToBeRead.ID)
		fileSection := models.FileSection{
			ID:                    uuid.New().String(),
			TaskID:                taskDetails.ID,
			FileID:                fileToBeRead.ID,
			SectionSequenceNumber: sectionSequenceNumber,
			OriginalFilePurpose:   fileToBeRead.FilePurpose,
			SectionRows:           sectionRows,
			ComparisonPairs:       taskDetails.ComparisonPairs,
			ColumnHeaders:         columnHeaders,
			ReconConfig:           taskDetails.ReconConfig,
			IsLastSection:         false,
		}
		//increment the section sequence number
		sectionSequenceNumber++
		err = publishSectionToStream(ctx, fileToBeRead, fileSection)
		if err != nil {
			return err
		}

		//Add empty last file section with isLastChunk=true
		//to signal channel close
		lastFileSection := models.FileSection{
			ID:                    uuid.New().String(),
			FileID:                fileToBeRead.ID,
			TaskID:                taskDetails.ID,
			SectionSequenceNumber: sectionSequenceNumber,
			OriginalFilePurpose:   fileToBeRead.FilePurpose,
			SectionRows:           []models.FileSectionRow{},
			ComparisonPairs:       taskDetails.ComparisonPairs,
			ColumnHeaders:         columnHeaders,
			ReconConfig:           taskDetails.ReconConfig,
			IsLastSection:         true,
		}
		err = publishSectionToStream(ctx, fileToBeRead, lastFileSection)
		if err != nil {
			return err
		}
	} else {
		log.Printf("Inputing last fileSection for file [%v]", fileToBeRead.ID)
		//Add empty last file section with isLastChunk=true
		//to signal channel close
		lastFileSection := models.FileSection{
			ID:                    uuid.New().String(),
			FileID:                fileToBeRead.ID,
			TaskID:                taskDetails.ID,
			SectionSequenceNumber: sectionSequenceNumber,
			OriginalFilePurpose:   fileToBeRead.FilePurpose,
			SectionRows:           []models.FileSectionRow{},
			ComparisonPairs:       taskDetails.ComparisonPairs,
			ColumnHeaders:         columnHeaders,
			ReconConfig:           taskDetails.ReconConfig,
			IsLastSection:         true,
		}
		err = publishSectionToStream(ctx, fileToBeRead, lastFileSection)
		if err != nil {
			return err
		}
	}

	return nil
}

func publishSectionToStream(ctx context.Context, fileToBeRead models.FileToBeRead, fileSection models.FileSection) error {
	log.Printf("Publishing SeqNum: [%v] File: [%v]", fileSection.SectionSequenceNumber, fileSection.FileID)
	err := fileToBeRead.ReadFileResultsStream.PublishToTopic(
		ctx,
		fileToBeRead.ID,
		&fileSection,
	)

	if err != nil {
		err := fmt.Errorf("error on publishing to topic: [%v], fileId: [%v], SectionId: [%v]",
			err,
			fileSection.FileID,
			fileSection.SectionSequenceNumber,
		)
		return err
	}
	return nil
}

func determineColumnHeaders(fileToBeRead models.FileToBeRead, rowNumber uint64, record []string) (columnHeaders []string) {
	if fileToBeRead.FileMetadata.HasHeaderRow && rowNumber == 0 {
		columnHeaders = record
	} else {
		for i := 0; i < len(record); i++ {
			columnName := fmt.Sprintf("column_%d", i+1)
			columnHeaders = append(columnHeaders, columnName)
		}
	}
	return columnHeaders
}
