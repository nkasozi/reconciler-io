package reconstruction

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reconciler.io/models"
	"reconciler.io/models/enums/file_purpose"
	"testing"
)

func TestFileReconstructionActivity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "File Reconstruction Activity Suite")
}

var _ = Describe("CheckIfFullFileHasBeenReconstructed", func() {

	Context("when fileSections is empty", func() {
		It("should return false", func() {
			Expect(checkIfFullFileHasBeenReconstructed(
				[]models.FileSection{},
			)).To(BeFalse())
		})
	})

	Context("when fileSections has only one section", func() {
		It("should return false", func() {
			sections := []models.FileSection{
				{
					ID:                    "1",
					SectionSequenceNumber: 1,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
			}
			Expect(checkIfFullFileHasBeenReconstructed(sections)).To(BeFalse())
		})
	})

	Context("when fileSections has all sections", func() {
		It("should return true", func() {
			sections := []models.FileSection{
				{
					ID:                    "2",
					SectionSequenceNumber: 2,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         true,
				},
				{
					ID:                    "1",
					SectionSequenceNumber: 1,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
			}
			Expect(checkIfFullFileHasBeenReconstructed(sections)).To(BeTrue())
		})
	})

	Context("when fileSections has more than one section but some sections are missing", func() {
		It("should return false", func() {
			sections := []models.FileSection{
				{
					ID:                    "2",
					SectionSequenceNumber: 2,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
				{
					ID:                    "2",
					SectionSequenceNumber: 5,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         true,
				},
				{
					ID:                    "1",
					SectionSequenceNumber: 1,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
			}
			Expect(checkIfFullFileHasBeenReconstructed(sections)).To(BeFalse())
		})
	})

	Context("when fileSections has no section beginning file", func() {
		It("should return false", func() {
			sections := []models.FileSection{
				{
					ID:                    "2",
					SectionSequenceNumber: 2,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
				{
					ID:                    "3",
					SectionSequenceNumber: 3,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
				{
					ID:                    "4",
					SectionSequenceNumber: 4,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
			}
			Expect(checkIfFullFileHasBeenReconstructed(sections)).To(BeFalse())
		})
	})

	Context("when fileSections has more than one section but has no last sections", func() {
		It("should return false", func() {
			sections := []models.FileSection{
				{
					ID:                    "2",
					SectionSequenceNumber: 2,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
				{
					ID:                    "3",
					SectionSequenceNumber: 3,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
				{
					ID:                    "1",
					SectionSequenceNumber: 1,
					OriginalFilePurpose:   file_purpose.PrimaryFile,
					ReconConfig:           models.ReconciliationConfigs{},
					IsLastSection:         false,
				},
			}
			Expect(checkIfFullFileHasBeenReconstructed(sections)).To(BeFalse())
		})
	})
})
