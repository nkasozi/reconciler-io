package reconciliation

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/recon_status"
)

var _ = Describe("FileReconciliationActivity", func() {
	var (
		primarySection           models.FileSection
		comparisonSectionsStream chan models.FileSection
		reconConfig              models.ReconciliationConfigs
	)

	BeforeEach(func() {
		// Initialize the primary section
		primarySection = models.FileSection{
			OriginalFilePurpose: file_purpose.PrimaryFile,
			SectionRows: []models.FileSectionRow{
				{
					RowNumber: 1,
					// Other fields as needed
				},
			},
			// Other fields as needed
		}

		// Initialize the comparison sections stream
		comparisonSectionsStream = make(chan models.FileSection, 2)
		comparisonSectionsStream <- models.FileSection{OriginalFilePurpose: file_purpose.ComparisonFile} // Add comparison sections as needed
		comparisonSectionsStream <- models.FileSection{OriginalFilePurpose: file_purpose.ComparisonFile} // Add comparison sections as needed

		// Initialize the reconciliation configuration
		reconConfig = models.ReconciliationConfigs{
			ShouldReconciliationBeCaseSensitive: true,
			// Other fields as needed
		}
	})

	AfterEach(func() {
		close(comparisonSectionsStream)
	})

	Describe("ReconcileFileSection", func() {
		Context("with valid primary and comparison sections", func() {
			It("should reconcile the primary section successfully", func() {
				reconciledSection, err := ReconcileFileSection(primarySection, comparisonSectionsStream, reconConfig)
				Expect(err).NotTo(HaveOccurred())
				Expect(reconciledSection.SectionRows[0].ReconResult).To(Equal(recon_status.Successfull))
				// Additional assertions based on the expected reconciliation result
				// ...
			})
		})

		Context("with invalid primary section type", func() {
			It("should return an error", func() {
				primarySection.OriginalFilePurpose = file_purpose.PrimaryFile
				_, err := ReconcileFileSection(primarySection, comparisonSectionsStream, reconConfig)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("primary section must be of type PrimaryFile"))
			})
		})

		Context("with invalid comparison section type", func() {
			It("should return an error", func() {
				// Set an invalid comparison section type in the comparisonSectionsStream
				comparisonSectionsStream <- models.FileSection{OriginalFilePurpose: file_purpose.PrimaryFile}

				_, err := ReconcileFileSection(primarySection, comparisonSectionsStream, reconConfig)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("comparison section must be of type ComparisonFile"))
			})
		})
	})
})
