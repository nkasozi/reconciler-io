package pre_processing

import (
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"path/filepath"
	"reconciler.io/models"
)

var _ = Describe("FileReaderActivity", func() {
	var (
		fileToBeRead models.FileToBeRead
		sectionSize  int
	)

	BeforeEach(func() {
		fileToBeRead = models.FileToBeRead{
			ID:            "test-pre-processing-id",
			FileExtension: models.Csv,
			FilePath:      filepath.Join("testdata", "test.csv"),
			// Other fields as needed
		}
		sectionSize = 5
	})

	Describe("ReadFileIntoChannel", func() {
		Context("with a valid CSV pre-processing and section size", func() {
			It("should read the pre-processing and return pre-processing sections", func() {
				fileSections, err := ReadFileIntoChannel(context.Background(), fileToBeRead, sectionSize)
				Expect(err).NotTo(HaveOccurred())
				Expect(fileSections).To(HaveLen(2))
				Expect(fileSections[0].SectionRows).To(HaveLen(5))
				Expect(fileSections[1].SectionRows).To(HaveLen(1))
			})
		})

		Context("with an invalid section size", func() {
			It("should return an error", func() {
				_, err := ReadFileIntoChannel(context.Background(), fileToBeRead, 0)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("invalid pre-processing details or section size"))
			})
		})

		Context("with an unsupported pre-processing extension", func() {
			It("should return an error", func() {
				fileToBeRead.FileExtension = "txt"
				_, err := ReadFileIntoChannel(context.Background(), fileToBeRead, sectionSize)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("unsupported pre-processing extension"))
			})
		})
	})
})
