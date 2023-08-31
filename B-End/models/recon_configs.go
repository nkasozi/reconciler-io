package models

type ReconciliationConfigs struct {
	ShouldCheckForDuplicateRecordsInComparisonFile bool
	ShouldReconciliationBeCaseSensitive            bool
	ShouldIgnoreWhiteSpace                         bool
	ShouldDoReverseReconciliation                  bool
}
