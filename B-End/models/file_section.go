package models

import (
	"reconciler.io/models/enums/file_purpose"
	"reconciler.io/models/enums/recon_status"
)

type FileSection struct {
	ID                    string
	TaskID                string
	FileID                string
	SectionSequenceNumber int
	OriginalFilePurpose   file_purpose.FilePurposeType
	SectionRows           []FileSectionRow
	ComparisonPairs       []ComparisonPair
	ColumnHeaders         []string
	ReconConfig           ReconciliationConfigs
	IsLastSection         bool
}

func (s *FileSection) AllRowsAreReconciled() bool {
	for _, fileSectionRow := range s.SectionRows {
		if fileSectionRow.ReconResult == recon_status.Pending {
			return false
		}
	}
	return true
}

type FileSectionRow struct {
	RowNumber            uint64
	RawData              string
	ParsedColumnsFromRow []string
	ReconResult          recon_status.ReconciliationStatus
	ReconResultReasons   []string
}
