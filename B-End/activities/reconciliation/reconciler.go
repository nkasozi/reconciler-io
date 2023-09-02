package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reconciler.io/constants"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/recon_status"
	"reconciler.io/utils"
	"sync"
)

func BeginFileReconciliation(
	primaryFile models.FileToBeRead,
	comparisonFile models.FileToBeRead,
	reconTaskDetails models.ReconTaskDetails,
) error {

	//create a consumer on the primary file sections stream
	log.Printf("creating primaryFileSectionsStreamConsumer for file: [%v]", primaryFile.ID)
	consumerId := primaryFile.ID
	topicName := primaryFile.ID
	primaryFileSectionsStreamConsumer, err := primaryFile.ReadFileResultsStream.CreateStreamConsumer(
		utils.NewContextWithDefaultTimeout(),
		constants.PRIMARY_FILE_SECTIONS_STREAM_NAME,
		topicName,
		consumerId,
	)

	//err on creating consumer
	if err != nil {
		log.Printf("Error creating PrimarySectionStreamConsumer: %v", err)
		return err
	}

	log.Printf("successfully created primaryFileSectionsStreamConsumer for file: [%v]", primaryFile.ID)

	var wg sync.WaitGroup
	for {
		//for each primary file section
		//we will spin up a separate consumer
		//on the comparison file stream
		wg.Add(1)

		log.Printf("Waiting new primary fileSection. fileID: [%v]", primaryFile.ID)
		primaryFileSection, err := primaryFileSectionsStreamConsumer.FetchNext()

		if err != nil {
			log.Printf("Error getting next PrimarySection: %v", err)
			break
		}

		log.Printf("Begining reconciliation for PrimaryFileSection:[%v]", primaryFileSection.SectionSequenceNumber)

		go func(
			primaryFileSection models.FileSection,
			comparisonFileSectionsStream models.StreamProvider,
			fileReconstructionChannel models.StreamProvider,
			reconciliationConfigs models.ReconciliationConfigs,
			comparisonFileID string,
			wg *sync.WaitGroup,
		) {
			// Recovery mechanism
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Reconciliation goroutine panicked with error: %v", r)
				}
			}()

			defer wg.Done()

			log.Printf("Reconciling Primary FileSectionID: [%v], FileID: [%v]",
				primaryFileSection.SectionSequenceNumber,
				primaryFileSection.FileID,
			)

			//reconcile the section from the primary file
			reconciledFileSection, err := ReconcileFileSection(
				primaryFileSection,
				comparisonFileSectionsStream,
				reconciliationConfigs,
				comparisonFileID,
			)

			//error on reconciliation
			if err != nil {
				log.Printf(
					"Error on file section reconciliation. "+
						"Seq Number: [{%d}], FileID: [{%s}]",
					primaryFileSection.SectionSequenceNumber,
					primaryFileSection.FileID,
				)
				return
			}

			log.Printf(
				"Finished file section reconciliation. "+
					"TaskID:[%v] ,Seq Number: [{%v}], FileID: [{%v}]",
				primaryFileSection.TaskID,
				primaryFileSection.SectionSequenceNumber,
				primaryFileSection.FileID,
			)

			//if there are any rows still pending reconciliation in the fileSection
			//we mark them as failed with the reason that no matching row found
			reconciledFileSection = giveEachRowAFinalReconStatus(reconciledFileSection)

			//publish the reconciled file section
			//to the reconstruction channel
			toBeReconstructedStreamTopicName := fmt.Sprintf("Reconstruct-%v", reconciledFileSection.TaskID)
			err = fileReconstructionChannel.PublishToTopic(
				context.Background(),
				toBeReconstructedStreamTopicName,
				reconciledFileSection,
			)

			//failed to publish
			if err != nil {
				log.Printf(
					"Failed to publish to reconstruction channel"+
						"TaskID:[%v] ,Seq Number: [{%v}], FileID: [{%v}]",
					primaryFileSection.TaskID,
					primaryFileSection.SectionSequenceNumber,
					primaryFileSection.FileID,
				)
				return
			}
		}(
			*primaryFileSection,
			comparisonFile.ReadFileResultsStream,
			reconTaskDetails.FileToBeReconstructedChannel,
			reconTaskDetails.ReconConfig,
			comparisonFile.ID,
			&wg,
		)
	}
	// Wait for all recon go routines
	// to finish
	wg.Wait()

	// clean up the consumers that were created
	err = primaryFile.ReadFileResultsStream.DeleteStreamConsumer(
		utils.NewContextWithDefaultTimeout(),
		constants.PRIMARY_FILE_SECTIONS_STREAM_NAME,
		consumerId,
	)

	if err != nil {
		log.Printf("Error deleting PrimaryFileSectionConsumer: [%v], Error: %v", consumerId, err)
	} else {
		log.Printf("Successfully deleted PrimaryFileSectionConsumer: [%v]", consumerId)
	}

	return nil
}

func giveEachRowAFinalReconStatus(reconciledFileSection models.FileSection) models.FileSection {
	finalReconciledSectionRows := make([]models.FileSectionRow, 0)
	for _, fileSectionRow := range reconciledFileSection.SectionRows {
		if fileSectionRow.ReconResult == recon_status.Pending {
			fileSectionRow.ReconResult = recon_status.Failed
			fileSectionRow.ReconResultReasons = []string{
				"no matching record found in the entire comparison file",
			}
			finalReconciledSectionRows = append(finalReconciledSectionRows, fileSectionRow)
		} else {
			finalReconciledSectionRows = append(finalReconciledSectionRows, fileSectionRow)
		}
	}
	reconciledFileSection.SectionRows = finalReconciledSectionRows
	return reconciledFileSection
}

// ReconcileFileSection reconciles a given primary pre-processing section with comparison pre-processing sections.
func ReconcileFileSection(
	primarySection models.FileSection,
	comparisonSectionsStream models.StreamProvider,
	reconConfig models.ReconciliationConfigs,
	comparisonFileID string,
) (models.FileSection, error) {
	if primarySection.OriginalFilePurpose != file_purpose.PrimaryFile {
		return models.FileSection{}, errors.New("primary section must be of type PrimaryFile")
	}
	consumerId := fmt.Sprintf("%v-%v", primarySection.SectionSequenceNumber, primarySection.FileID)
	comparisonSectionsStreamConsumer, err := comparisonSectionsStream.CreateStreamConsumer(
		utils.NewContextWithDefaultTimeout(),
		constants.COMPARISON_FILE_SECTIONS_STREAM_NAME,
		comparisonFileID,
		consumerId,
	)
	if err != nil {
		return primarySection, err
	}

	for {
		log.Printf("Waiting for ComparisonFileSection. "+
			"PrimaryFileSectionID: [%v], FileID: [%v]",
			primarySection.SectionSequenceNumber,
			primarySection.FileID,
		)
		comparisonSection, err := comparisonSectionsStreamConsumer.FetchNext()

		if err != nil {
			log.Printf("Error getting FetchNext ComparsionSection: %v", err)
			break
		}

		log.Printf(
			"Recieved ComparisonFileSection [%v] "+
				"for PrimaryFileSection [%v], FileID: [%v]",
			comparisonSection.SectionSequenceNumber,
			primarySection.SectionSequenceNumber,
			primarySection.FileID,
		)

		if comparisonSection.OriginalFilePurpose != file_purpose.ComparisonFile {
			return models.FileSection{}, errors.New("comparison section must be of type ComparisonFile")
		}

		log.Printf(
			"Reconciling PrimaryFileSection [%v] "+
				"with ComparisonFileSection [%v], FileID: [%v]",
			primarySection.SectionSequenceNumber,
			comparisonSection.SectionSequenceNumber,
			primarySection.FileID,
		)

		primarySection = reconcileWithComparisonSection(primarySection, *comparisonSection, reconConfig)

		if primarySection.AllRowsAreReconciled() {
			log.Printf("Finished reconciling ALL rows for primaryFileSectionID:[%v], FileID: [%v]",
				primarySection.SectionSequenceNumber,
				primarySection.FileID,
			)
			break
		}

		if comparisonSection.IsLastSection {
			log.Printf("Finished reconciling primaryFileSectionID:[%v], FileID: [%v]",
				primarySection.SectionSequenceNumber,
				primarySection.FileID,
			)
			break
		}
	}

	err = comparisonSectionsStream.DeleteStreamConsumer(
		utils.NewContextWithDefaultTimeout(),
		constants.COMPARISON_FILE_SECTIONS_STREAM_NAME,
		consumerId,
	)

	if err != nil {
		log.Printf("Error deleting ComparsionFileSectionConsumer: [%v], Error: %v", consumerId, err)
	} else {
		log.Printf("Successfully deleted ComparsionFileSectionConsumer: [%v]", consumerId)
	}

	return primarySection, nil
}

func reconcileWithComparisonSection(primarySection models.FileSection, comparisonSection models.FileSection, reconConfig models.ReconciliationConfigs) models.FileSection {
	for i, primaryRow := range primarySection.SectionRows {
		for _, comparisonRow := range comparisonSection.SectionRows {
			found, rowReconStatus, reasons := isRowMatch(
				primaryRow,
				comparisonRow,
				primarySection.ComparisonPairs,
				reconConfig,
				primarySection.ColumnHeaders,
			)
			if found {
				log.Printf(
					"Found match between PrimaryFileSectionRow [%v] "+
						"and ComparisonFileSectionRow [%v]",
					primaryRow.RowNumber,
					comparisonRow.RowNumber,
				)
				primarySection.SectionRows[i].ReconResult = rowReconStatus
				primarySection.SectionRows[i].ReconResultReasons = append(
					primarySection.SectionRows[i].ReconResultReasons,
					reasons...,
				)
				break
			} else {
				log.Printf(
					"No match between PrimaryFileSectionRow [%v] "+
						"and ComparisonFileSectionRow [%v]",
					primaryRow.RowNumber,
					comparisonRow.RowNumber,
				)
				continue
			}
		}
	}

	return primarySection
}

func isRowMatch(
	primaryRow models.FileSectionRow,
	comparisonRow models.FileSectionRow,
	comparisonPairs []models.ComparisonPair,
	reconConfig models.ReconciliationConfigs,
	columnHeaders []string,
) (bool, recon_status.ReconciliationStatus, []string) {
	//check if this is supposed to be the same row in both files
	//by using the comparison pair is row identifier
	//a row identifier can be made up of 2 or more properties
	rowIdentifierComparisonPairs, nonRowIdComparisonPairs := getRowIdentifierComparisonPairs(comparisonPairs)

	//first make sure all the values in the rowIdCompPairs
	//are the same. if they are the same then we can know
	//that the rest of the other values in this row must match
	for _, pair := range rowIdentifierComparisonPairs {
		primaryValue := primaryRow.ParsedColumnsFromRow[pair.PrimaryFileColumnIndex]
		comparisonValue := comparisonRow.ParsedColumnsFromRow[pair.ComparisonFileColumnIndex]

		if reconConfig.ShouldReconciliationBeCaseSensitive {
			if primaryValue != comparisonValue {
				return false, recon_status.Pending, nil
			}
		} else {
			if primaryValue != comparisonValue {
				return false, recon_status.Pending, nil
			}
		}
	}

	//now we can check for the other values in comp pairs
	for _, pair := range nonRowIdComparisonPairs {
		primaryValue := primaryRow.ParsedColumnsFromRow[pair.PrimaryFileColumnIndex]
		comparisonValue := comparisonRow.ParsedColumnsFromRow[pair.ComparisonFileColumnIndex]

		if reconConfig.ShouldReconciliationBeCaseSensitive {
			if primaryValue != comparisonValue {
				reason := fmt.Sprintf(
					"RowMismatchFound. \n"+
						"PrimaryFileRow: [%v] PrimaryFileColumn: [%v] \n"+
						"ComparisonFileRow: [%v] ComparisonFileColumn: [%v] \n"+
						"PrimaryFile value: [%v] \n"+
						"ComparisonFile value: [%v]\n",
					primaryRow.RowNumber,
					columnHeaders[pair.PrimaryFileColumnIndex],
					comparisonRow.RowNumber,
					columnHeaders[pair.ComparisonFileColumnIndex],
					primaryValue,
					comparisonValue,
				)
				return true, recon_status.Failed, []string{reason}
			}
		} else {
			if primaryValue != comparisonValue {
				reason := fmt.Sprintf(
					"RowMismatchFound. \n"+
						"PrimaryFileRow: [%v] PrimaryFileColumn: [%v] \n"+
						"ComparisonFileRow: [%v] ComparisonFileColumn: [%v] \n"+
						"PrimaryFile value: [%v] \n"+
						"ComparisonFile value: [%v]\n",
					primaryRow.RowNumber,
					columnHeaders[pair.PrimaryFileColumnIndex],
					comparisonRow.RowNumber,
					columnHeaders[pair.ComparisonFileColumnIndex],
					primaryValue,
					comparisonValue,
				)
				return true, recon_status.Failed, []string{reason}
			}
		}
	}

	reason := fmt.Sprintf(
		"RowMatchFound. \n"+
			"PrimaryFile Row: [%v] \n"+
			"ComparisonFile Row: [%v] \n",
		primaryRow.RowNumber,
		comparisonRow.RowNumber,
	)
	return true, recon_status.Successfull, []string{reason}
}

func getRowIdentifierComparisonPairs(
	comparisonPairs []models.ComparisonPair,
) (
	rowIdComparisonPairs []models.ComparisonPair,
	nonRowIdComparisonPairs []models.ComparisonPair,
) {
	for _, comparisonPair := range comparisonPairs {
		if comparisonPair.IsRowIdentifier {
			rowIdComparisonPairs = append(rowIdComparisonPairs, comparisonPair)
		} else {
			nonRowIdComparisonPairs = append(nonRowIdComparisonPairs, comparisonPair)
		}

	}
	return
}
