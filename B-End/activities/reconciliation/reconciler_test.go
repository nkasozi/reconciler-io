package reconciliation

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/recon_status"
	"testing"
)

func TestBeginFileReconciliationActivity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "File Reconstruction Activity Suite")
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
			primaryFileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			comparisonFileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			expectedReconResults = models.FileSection{
				ID:                    "1",
				SectionSequenceNumber: 1,
				OriginalFilePurpose:   file_purpose.PrimaryFile,
				SectionRows: []models.FileSectionRow{
					{
						RowNumber: 1,
						ParsedColumnsFromRow: []string{
							"data1", "data2",
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

		It("should reconcile each row based on the comparison", func() {
			actualReconResults := reconcileWithComparisonSection(
				primaryFileSection,
				comparisonFileSection,
				reconConfig,
			)
			Expect(actualReconResults).To(Equal(expectedReconResults))
		})
	})

	Context("when the file section and comparison section have different number of rows", func() {
		BeforeEach(func() {
			primaryFileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
			}

			comparisonFileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			expectedReconResults = models.FileSection{}

			reconConfig = models.ReconciliationConfigs{}
		})

		It("should reconcile as many rows as possible and mark the remaining rows as not reconciled", func() {
			actualReconResults := reconcileWithComparisonSection(
				primaryFileSection,
				comparisonFileSection,
				reconConfig,
			)
			Expect(actualReconResults).To(Equal(expectedReconResults))
		})
	})
})
