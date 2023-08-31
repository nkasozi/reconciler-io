package reconstruction

import (
	"encoding/csv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"os"
	"reconciler.io/models"
	"testing"
)

func TestFileReconstructionActivity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "File Reconstruction Activity Suite")
}

var _ = Describe("ReconstructFile", func() {
	Context("when provided with valid pre-processing sections", func() {
		It("should reconstruct the pre-processing in the correct order", func() {
			// Define test pre-processing sections
			fileSectionsStream := make(chan models.FileSection, 3)
			fileSectionsStream <- models.FileSection{SectionSequenceNumber: 2, ColumnHeaders: []string{"A", "B"}, SectionRows: []models.FileSectionRow{{ParsedColumnsFromRow: []string{"2A", "2B"}}}}
			fileSectionsStream <- models.FileSection{SectionSequenceNumber: 1, ColumnHeaders: []string{"A", "B"}, SectionRows: []models.FileSectionRow{{ParsedColumnsFromRow: []string{"1A", "1B"}}}}
			fileSectionsStream <- models.FileSection{SectionSequenceNumber: 3, ColumnHeaders: []string{"A", "B"}, SectionRows: []models.FileSectionRow{{ParsedColumnsFromRow: []string{"3A", "3B"}}}}
			close(fileSectionsStream)

			outputPath := "test_output.csv"
			defer os.Remove(outputPath)

			// Call the ReconstructFile function
			err := ReconstructFile(fileSectionsStream, outputPath)
			Expect(err).NotTo(HaveOccurred())

			// Open and read the reconstructed CSV pre-processing
			file, err := os.Open(outputPath)
			Expect(err).NotTo(HaveOccurred())
			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			Expect(err).NotTo(HaveOccurred())

			// Verify the order and content of the reconstructed pre-processing
			assert.Equal(GinkgoT(), []string{"A", "B"}, records[0])
			assert.Equal(GinkgoT(), []string{"1A", "1B"}, records[1])
			assert.Equal(GinkgoT(), []string{"2A", "2B"}, records[2])
			assert.Equal(GinkgoT(), []string{"3A", "3B"}, records[3])
		})
	})
})
