package models

type ComparisonPair struct {
	PrimaryFileColumnIndex    int
	ComparisonFileColumnIndex int
	IsRowIdentifier           bool
}
