package recon_status

type ReconciliationStatus string

const (
	Failed      ReconciliationStatus = "Failed"
	Successfull ReconciliationStatus = "Successfull"
	Pending     ReconciliationStatus = "Pending"
)
