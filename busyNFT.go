package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// BusyNFT provides functions for transferring NFT's between accounts
type BusyNFT struct {
	contractapi.Contract
}

// BusyNFTMeta holds the metadata of the NFT.
type BusyNFTMeta struct {
	Description string      `json:"description"`
	Image       string      `json:"image"`
	Properties  interface{} `json:"properties"`
}

// BusyNft stores the current NFT holder account, time of creation, metadata
type BusyNft struct {
	Account   string      `json:"account"`
	CreatedAT time.Time   `json:"created_at"`
	MetaData  BusyNFTMeta `json:"metadata"`
}

// Mint creates a unique nft and assigns them to account.
func (s *BusyNFT) Mint(ctx contractapi.TransactionContextInterface, account string, nftName string, metadata BusyNFTMeta) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if nftName == BUSY_COIN_SYMBOL {
		response.Message = "NFT name cannot be BUSY"
		logger.Error(response.Message)
		return response, generateError(412, "SMIN001", response.Message)
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	exp := regexp.MustCompile(`^[\w]+([-\s]{1}[\w]+)*$`)
	symbolLength := regexp.MustCompile(`^.{3,5}$`)
	// checking for tokenSymbol
	if !exp.MatchString(nftName) || !symbolLength.MatchString(nftName) {
		response.Message = "Invalid nftName"
		logger.Error(response.Message)
		return response, generateError(412, "SMIN002", response.Message)
	}

	// checking if the nftname already exists
	busyNftAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("busy-nft-%s", nftName))
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SMIN003", response.Message)
	}
	if busyNftAsBytes != nil {
		response.Message = "NFT already exists"
		logger.Info(response.Message)
		return response, generateError(409, "SMIN004", response.Message)
	}

	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(account)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SMIN005", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Account %s does not exist", account)
		logger.Error(response.Message)
		return response, generateError(404, "SMIN006", response.Message)
	}

	if metadata.Image == "" || metadata.Description == "" {
		response.Message = "You must input metadata"
		logger.Error(response.Message)
		return response, generateError(412, "SMIN007", response.Message)
	}

	properties, converted := metadata.Properties.(map[string]interface{})
	if converted {
		for key, value := range properties {
			val, ok := value.(string)
			if ok && val == "" {
				response.Message = fmt.Sprintf("%s should not be empty in properties", key)
				logger.Error(response.Message)
				return response, generateError(412, "SMIN008", response.Message)
			}
		}
	}

	balance, _ := getBalanceHelper(ctx, account, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(bigTxFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to mint new NFT", account)
		logger.Error(response.Message)
		return response, generateError(402, "SMIN009", response.Message)
	}
	err = txFeeHelper(ctx, account, BUSY_COIN_SYMBOL, bigTxFee.String(), "busyNft")
	if err != nil {
		response.Message = "Error while burning mint fee"
		logger.Error(response.Message)
		return response, generateError(500, "SMIN010", response.Message)
	}

	busyNft := BusyNft{
		Account:   account,
		CreatedAT: time.Now(),
		MetaData:  metadata,
	}
	busyNftAsBytes, _ = json.Marshal(&busyNft)
	err = ctx.GetStub().PutState(fmt.Sprintf("busy-nft-%s", nftName), busyNftAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SMIN003", response.Message)
	}

	// sending the balance event
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: account,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: bigTxFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Data = busyNft
	response.Success = true
	response.Message = "NFT has been successfully minted"
	return response, nil
}

// TransferFrom transfers tokens from sender account to recipient account
func (s *BusyNFT) Transfer(ctx contractapi.TransactionContextInterface, sender string, recipient string, nftName string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// checking if the token already exists
	busyNftAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("busy-nft-%s", nftName))
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA001", response.Message)
	}
	if busyNftAsBytes == nil {
		response.Message = "NFT does not exists"
		logger.Info(response.Message)
		return response, generateError(404, "STRA002", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while Validating default credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	if sender == recipient {
		return nil, generateError(409, "STRA003", "You cannot transfer to yourself")
	}
	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA004", response.Message)
	}
	senderDefaultAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA005", response.Message)
	}

	if sender != senderDefaultAddress {
		response.Message = fmt.Sprintf("Default walletId do not match %s %s", sender, senderDefaultAddress)
		logger.Error(response.Message)
		return response, generateError(409, "STRA006", response.Message)
	}

	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(recipient)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA007", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", recipient)
		logger.Error(response.Message)
		return response, generateError(404, "STRA008", response.Message)
	}

	busyNft := BusyNft{}
	err = json.Unmarshal(busyNftAsBytes, &busyNft)
	if err != nil {
		response.Message = fmt.Sprintf("Error while ummarshalling the data: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA009", response.Message)
	}

	if busyNft.Account != senderDefaultAddress {
		response.Message = fmt.Sprintf("%s is not owner of %s", senderDefaultAddress, nftName)
		logger.Error(response.Message)
		return response, generateError(403, "STRA010", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, senderDefaultAddress, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(bigTxFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to transfer NFT", senderDefaultAddress)
		logger.Error(response.Message)
		return response, generateError(402, "STRA011", response.Message)
	}
	err = txFeeHelper(ctx, senderDefaultAddress, BUSY_COIN_SYMBOL, bigTxFee.String(), "busynftTransfer")
	if err != nil {
		response.Message = "Error while burning transaction fee"
		logger.Error(response.Message)
		return response, generateError(500, "STRA012", response.Message)
	}

	busyNft.Account = recipient
	busyNftAsBytes, _ = json.Marshal(&busyNft)
	err = ctx.GetStub().PutState(fmt.Sprintf("busy-nft-%s", nftName), busyNftAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STRA013", response.Message)
	}
	// Check whether operator is owner or approved
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: sender,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: bigTxFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	response.Data = busyNft
	response.Message = "Request to transfer the NFT has been successfully accepted"
	response.Success = true
	return response, nil
}

// GetCurrentOwner retrieves the current owner of busyNft
func (s *BusyNFT) GetCurrentOwner(ctx contractapi.TransactionContextInterface, nftName string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// checking if the NFT already exists
	busyNftAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("busy-nft-%s", nftName))
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "COWN001", response.Message)
	}
	if busyNftAsBytes == nil {
		response.Message = "NFT does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "COWN002", response.Message)
	}
	busyNft := BusyNft{}
	err = json.Unmarshal(busyNftAsBytes, &busyNft)
	if err != nil {
		response.Message = fmt.Sprintf("Error while ummarshalling the data: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "COWN003", response.Message)
	}
	response.Data = busyNft.Account
	response.Message = "Owner Successfully fetched"
	response.Success = true
	return response, nil
}

// UpdateNFTMetaData to update NFTMetaData
func (s *BusyNFT) UpdateNFTMetaData(ctx contractapi.TransactionContextInterface, nftName string, metadata BusyNFTMeta) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// putting the NFTMetaData
	busyNftAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("busy-nft-%s", nftName))
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USMD001", response.Message)
	}
	if busyNftAsBytes == nil {
		response.Message = "NFT does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "USMD002", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	busyNft := BusyNft{}
	err = json.Unmarshal(busyNftAsBytes, &busyNft)
	if err != nil {
		response.Message = fmt.Sprintf("Error while marshalling the data: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USMD003", response.Message)
	}

	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("failed to get common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USMD004", response.Message)
	}
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USMD005", response.Message)
	}

	if defaultWalletAddress != busyNft.Account {
		response.Message = fmt.Sprintf("The account %s is not the owner of %s", defaultWalletAddress, nftName)
		logger.Error(response.Message)
		return response, generateError(403, "USMD006", response.Message)
	}

	if metadata.Image == "" || metadata.Description == "" {
		response.Message = "You must input metadata"
		logger.Error(response.Message)
		return response, generateError(412, "USMD007", response.Message)
	}

	properties, converted := metadata.Properties.(map[string]interface{})
	if converted {
		for key, value := range properties {
			val, ok := value.(string)
			if ok && val == "" {
				response.Message = fmt.Sprintf("%s should not be empty in properties", key)
				logger.Error(response.Message)
				return response, generateError(412, "USMD008", response.Message)
			}
		}
	}

	balance, _ := getBalanceHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(bigTxFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to transfer NFT", defaultWalletAddress)
		logger.Error(response.Message)
		return response, generateError(402, "USMD009", response.Message)
	}
	err = txFeeHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL, bigTxFee.String(), "busynftTransfer")
	if err != nil {
		response.Message = "Error while burning transaction fee"
		logger.Error(response.Message)
		return response, generateError(500, "USMD010", response.Message)
	}

	busyNft.MetaData = metadata
	// Check whether operator is owner or approved
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultWalletAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: bigTxFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	// unmarshall and putting in state
	busyNftAsBytes, _ = json.Marshal(busyNft)
	err = ctx.GetStub().PutState(fmt.Sprintf("busy-nft-%s", nftName), busyNftAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USMD011", response.Message)
	}

	response.Data = metadata
	response.Message = "The NFT's metadata has been successfully updated"
	response.Success = true
	return response, nil
}
