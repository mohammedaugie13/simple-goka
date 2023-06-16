package event

type Event struct {
	WalletID  string  `json:"wallet_id"`
	Amount    float64 `json:"amount"`
	Timestamp int64   `json:"timestamp"`
}

type ResponseGateway struct {
	WalletID       string  `json:"wallet_id"`
	Amount         float64 `json:"amount"`
	Timestamp      int64   `json:"timestamp"`
	AboveThreshold bool    `json:"above_threshold"`
}

//type AboveThreshold struct {
//	IsAboveThreshold bool `json:"is_above_threshold"`
//}
