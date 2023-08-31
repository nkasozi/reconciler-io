package models

type ReconTaskDetails struct {
	ID                           string
	UserID                       string
	IsDone                       bool
	HasBegun                     bool
	ComparisonPairs              []ComparisonPair
	ReconConfig                  ReconciliationConfigs
	FileToBeReconstructedChannel StreamProvider `json:"-"`
	PrimaryFileID                string
	ComparisonFileID             string
}
