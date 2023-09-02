package reconciliation

import (
	"reconciler.io/models"
	"reconciler.io/models/enums/recon_status"
)

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("reconcileWithComparisonSection", func() {
	var (
		fileSection          models.FileSection
		comparisonSection    models.FileSection
		expectedReconResults []recon_status.ReconciliationStatus
		reconConfig          models.ReconciliationConfigs
	)

	BeforeEach(func() {
		fileSection = models.FileSection{}
		comparisonSection = models.FileSection{}
		expectedReconResults = []recon_status.ReconciliationStatus{}
		reconConfig = models.ReconciliationConfigs{}
	})

	Context("when the file section and comparison section have the same number of rows", func() {
		BeforeEach(func() {
			fileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			comparisonSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			expectedReconResults = []recon_status.ReconciliationStatus{
				recon_status.Successfull,
				recon_status.Successfull,
			}
		})

		It("should reconcile each row based on the comparison", func() {
			actualReconResults := reconcileWithComparisonSection(
				fileSection,
				comparisonSection,
				reconConfig,
			)
			Expect(actualReconResults).To(Equal(expectedReconResults))
		})
	})

	Context("when the file section and comparison section have different number of rows", func() {
		BeforeEach(func() {
			fileSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
			}

			comparisonSection.SectionRows = []models.FileSectionRow{
				{RawData: "data1"},
				{RawData: "data2"},
			}

			expectedReconResults = []recon_status.ReconciliationStatus{
				recon_status.Successfull,
				recon_status.Pending,
			}

			reconConfig = models.ReconciliationConfigs{}
		})

		It("should reconcile as many rows as possible and mark the remaining rows as not reconciled", func() {
			actualReconResults := reconcileWithComparisonSection(
				fileSection,
				comparisonSection,
				reconConfig,
			)
			Expect(actualReconResults).To(Equal(expectedReconResults))
		})
	})
})
