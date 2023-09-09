package reconciliation

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reconciler.io/models"
	"reconciler.io/models/enums/recon_status"
	"testing"
)

func TestBeginFileReconciliationActivity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "File Reconciliation Activity Suite")
}

var _ = Describe("reconcileWithComparisonSection", func() {
	var (
		primaryFileSection    models.FileSection
		comparisonFileSection models.FileSection
		expectedReconResults  models.FileSection
		reconConfig           models.ReconciliationConfigs
	)

	BeforeEach(func() {
		primaryFileSection = models.FileSection{}
		comparisonFileSection = models.FileSection{}
		expectedReconResults = models.FileSection{}
		reconConfig = models.ReconciliationConfigs{}
	})

	Context("when the file section and comparison section have the same number of rows", func() {
		BeforeEach(func() {
			primaryFileSection = models.FileSection{
				SectionRows: []models.FileSectionRow{
					{
						RowNumber: 0,
						ParsedColumnsFromRow: []string{
							"column_1", "column_2",
						},
						ReconResult: recon_status.Successfull,
						ReconResultReasons: []string{
							"RowMatchFound. \nPrimaryFile Row: [1] \nComparisonFile Row: [2] \n",
						},
					},
				},
				ComparisonPairs: []models.ComparisonPair{
					{
						PrimaryFileColumnIndex:    1,
						ComparisonFileColumnIndex: 2,
						IsRowIdentifier:           true,
					},
					{
						PrimaryFileColumnIndex:    2,
						ComparisonFileColumnIndex: 1,
						IsRowIdentifier:           false,
					},
				},
				ColumnHeaders: nil,
				ReconConfig: models.ReconciliationConfigs{
					ShouldCheckForDuplicateRecordsInComparisonFile: false,
					ShouldReconciliationBeCaseSensitive:            false,
					ShouldIgnoreWhiteSpace:                         false,
					ShouldDoReverseReconciliation:                  false,
				},
				IsLastSection: false,
			}
		})

		comparisonFileSection = models.FileSection{
			SectionRows: []models.FileSectionRow{
				{
					RowNumber:            0,
					RawData:              "column_2,column_1",
					ParsedColumnsFromRow: []string{"column_2", "column_1"},
					ReconResult:          recon_status.Pending,
					ReconResultReasons:   nil,
				},
			},
			ComparisonPairs: []models.ComparisonPair{
				{
					PrimaryFileColumnIndex:    1,
					ComparisonFileColumnIndex: 2,
					IsRowIdentifier:           true,
				},
				{
					PrimaryFileColumnIndex:    2,
					ComparisonFileColumnIndex: 1,
					IsRowIdentifier:           false,
				},
			},
			ColumnHeaders: nil,
			ReconConfig: models.ReconciliationConfigs{
				ShouldCheckForDuplicateRecordsInComparisonFile: false,
				ShouldReconciliationBeCaseSensitive:            false,
				ShouldIgnoreWhiteSpace:                         false,
				ShouldDoReverseReconciliation:                  false,
			},
			IsLastSection: false,
		}
	})

	expectedReconResults = models.FileSection{
		SectionRows: []models.FileSectionRow{
			{
				RowNumber: 0,
				ParsedColumnsFromRow: []string{
					"column_1", "column_2",
				},
				ReconResult: recon_status.Successfull,
				ReconResultReasons: []string{
					"RowMatchFound. \nPrimaryFile Row: [1] \nComparisonFile Row: [2] \n",
				},
			},
		},
		ComparisonPairs: []models.ComparisonPair{
			{
				PrimaryFileColumnIndex:    1,
				ComparisonFileColumnIndex: 2,
				IsRowIdentifier:           true,
			},
			{
				PrimaryFileColumnIndex:    2,
				ComparisonFileColumnIndex: 1,
				IsRowIdentifier:           false,
			},
		},
		ColumnHeaders: nil,
		ReconConfig: models.ReconciliationConfigs{
			ShouldCheckForDuplicateRecordsInComparisonFile: false,
			ShouldReconciliationBeCaseSensitive:            false,
			ShouldIgnoreWhiteSpace:                         false,
			ShouldDoReverseReconciliation:                  false,
		},
		IsLastSection: false,
	}

	It("should reconcile each row based on the comparison", func() {
		actualReconResults := reconcileWithComparisonSection(
			primaryFileSection,
			comparisonFileSection,
			reconConfig,
		)
		Expect(actualReconResults).To(Equal(expectedReconResults))
	})
})
