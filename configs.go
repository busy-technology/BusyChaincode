package main

type TokenIssueFee struct {
	// BusyCoins to deduct
	BUSY20 string `json:"busy20"`
	NFT    string `json:"nft"`
	GAME   string `json:"game"`
}
