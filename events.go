package main

type UserAddress struct {
	Address string `json:"address,omitempty"`
	Token   string `json:"token,omitempty"`
}

type BalanceEvent struct {
	UserAddresses  []UserAddress `json:"userAddresses"`
	TransactionFee string        `json:"transactionFee"`
	TransactionId  string        `json:"transactionId"`
}
