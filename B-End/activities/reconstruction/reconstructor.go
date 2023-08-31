package reconstruction

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reconciler.io/constants"
	"reconciler.io/models"
	"reconciler.io/utils"
	"sort"
	"strings"
)

func ReconstructFile(taskId string, reconstructFileSectionsStream models.StreamProvider, outputPath string) error {
	var fileSections []models.FileSection

	// Read all pre-processing sections from the stream
	toBeReconstructedStreamTopicName := fmt.Sprintf("Reconstruct-%v", taskId)

	reconstructFileSectionsStreamConsumer, err := reconstructFileSectionsStream.CreateStreamConsumer(
		utils.NewContextWithDefaultTimeout(),
		constants.FILE_RECONSTRUCTION_STREAM_NAME,
		toBeReconstructedStreamTopicName,
		taskId,
	)

	if err != nil {
		log.Printf("error on creating fileReconstructionStreamConsumer: %v", err)
		return err
	}

	for {
		section, err := reconstructFileSectionsStreamConsumer.FetchNext()

		if err != nil {
			log.Printf("error on getting next reconstruct fileSection: %v", err)
			continue
		}

		fileSections = append(fileSections, *section)

		hasBeenReconstructed := checkIfFullFileHasBeenReconstructed(fileSections)

		if hasBeenReconstructed {
			break
		}
	}

	// Create and open the output CSV pre-processing
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the column headers
	var columnHeaders = fileSections[0].ColumnHeaders
	columnHeaders = append(columnHeaders, "ReconResult")
	columnHeaders = append(columnHeaders, "ReconResultReasons")
	if len(fileSections) > 0 {
		err := writer.Write(columnHeaders)
		if err != nil {
			return err
		}
	}

	// Write the rows from each section
	for _, section := range fileSections {
		for _, row := range section.SectionRows {
			row.ParsedColumnsFromRow = append(row.ParsedColumnsFromRow, string(row.ReconResult))
			row.ParsedColumnsFromRow = append(row.ParsedColumnsFromRow, strings.Join(row.ReconResultReasons, ","))
			err := writer.Write(row.ParsedColumnsFromRow)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func checkIfFullFileHasBeenReconstructed(fileSections []models.FileSection) bool {
	//every file must have at least 2 file sections
	if len(fileSections) <= 1 {
		return false
	}

	// Sort the currently received file sections by section_sequence_number
	sort.Slice(fileSections, func(i, j int) bool {
		return fileSections[i].SectionSequenceNumber < fileSections[j].SectionSequenceNumber
	})

	// Firstly, check if the first file section seq number is one
	firstSection := fileSections[0]

	// if it isn't then it means we haven't yet received the
	// beginning of this file but maybe have received other sections
	// either way, we know the file has not been reconstructed
	if firstSection.SectionSequenceNumber != 1 {
		return false
	}

	// Check to see if the current file section's sequence number
	// is the previous file section's sequence number + 1
	for i := 0; i < len(fileSections)-1; i++ {
		if fileSections[i+1].SectionSequenceNumber != fileSections[i].SectionSequenceNumber+1 {
			return false
		}
	}

	// Lastly, check if the last file section has the IsLastSection flag is
	//set to true
	lastSection := fileSections[len(fileSections)-1]
	return lastSection.IsLastSection
}
