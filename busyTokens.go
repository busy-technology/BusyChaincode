package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

const balancePrefix = "account~tokenId~sender"
const approvalPrefix = "account~operator"
const tokenAddressPrefix = "0x"

const NFT_EVENT = "NFT"
const TOTAL_SUPPLY_KEY_NFT = "NFT~TOTAL~SUPPLY"

// BusyTokens provides functions for transferring tokens between accounts
type BusyTokens struct {
	contractapi.Contract
}

type TransferSingle struct {
	Operator     string `json:"operator"`
	From         string `json:"from"`
	To           string `json:"to"`
	Symbol       string `json:"symbol"`
	Value        string `json:"value"`
	TokenAddress string `json:"tokenAddress"`
}

type TransferBatch struct {
	Operator       string   `json:"operator"`
	From           string   `json:"from"`
	To             string   `json:"to"`
	Symbols        []string `json:"symbols"`
	Values         []string `json:"values"`
	TokenAddresses []string `json:"tokenAddresses"`
}

type ApprovalForAll struct {
	Owner    string `json:"owner"`
	Operator string `json:"operator"`
	Approved bool   `json:"approved"`
}

// TokenMetaData holds the metadata of the tokens Minted.
type TokenMetaData struct {
	Name        string      `json:"name,omitempty"`
	Type        string      `json:"type"`
	Description string      `json:"description,omitempty"`
	Logo        string      `json:"logo"`
	Properties  interface{} `json:"properties"`
	Website     string      `json:"website,omitempty"`
	SocialMedia string      `json:"socialMedia,omitempty"`
}

// BusyTokensInfo holds metadata and owner info
type BusyTokensInfo struct {
	Account      string        `json:"account"`
	CreatedAT    time.Time     `json:"created_at"`
	TokenAddress string        `json:"tokenAddress"`
	MetaData     TokenMetaData `json:"tokenMetadata"`
	TotalSupply  string        `json:"totalSupply"`
	TokenSymbol  string        `json:"tokenSymbol"`
}

// NFTEvent Holds data for NFT event sent out
type NFTEvent struct {
	UserAddresses  UserAddress    `json:"userAddress,omitempty"`
	NFTList        []NFTEventInfo `json:"nftEventInfo"`
	TransactionFee string         `json:"transactionFee"`
	TransactionId  string         `json:"transactionId"`
}

type NFTEventInfo struct {
	Account   string `json:"account"`
	Symbol    string `json:"symbol"`
	TokenType string `json:"tokenType"`
}

type MetaDataBatch struct {
	MetaData []TokenMetaData `json:"tokenMetaData"`
}

// Mint creates amount tokens of token type and assigns them to account.
func (s *BusyTokens) Mint(ctx contractapi.TransactionContextInterface, account string, symbol string, totalSupply string, metadata TokenMetaData) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	tokenAddress := generateTokenAddress(symbol)
	// checking if the token already exists
	busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN001", response.Message)
	}
	if busyTokensInfoAsBytes != nil {
		response.Message = "Token already exists"
		logger.Info(response.Message)
		return response, generateError(409, "MIN002", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	exp := regexp.MustCompile(`^[\w]+([-\s]{1}[\w]+)*$`)
	oneLetter := regexp.MustCompile(`.*.[a-zA-Z].*$`)

	tokenLength := regexp.MustCompile(`^.{3,20}$`)
	// checking for token Name
	if !exp.MatchString(metadata.Name) || !tokenLength.MatchString(metadata.Name) || !oneLetter.MatchString(metadata.Name) {
		response.Message = "Invalid token name"
		logger.Error(response.Message)
		return response, generateError(412, "MIN003", response.Message)
	}

	symbolLength := regexp.MustCompile(`^.{3,5}$`)
	// checking for tokenSymbol
	if !exp.MatchString(symbol) || !symbolLength.MatchString(symbol) || !oneLetter.MatchString(symbol) {
		response.Message = "Invalid token symbol"
		logger.Error(response.Message)
		return response, generateError(412, "MIN004", response.Message)
	}
	// check if token already exists
	exists, err := ifTokenExists(ctx, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN005", response.Message)
	}
	if exists {
		response.Message = fmt.Sprintf("Token with the same symbol %s already exists", symbol)
		logger.Error(response.Message)
		return response, generateError(409, "MIN006", response.Message)
	}

	if strings.ToUpper(symbol) == BUSY_COIN_SYMBOL || strings.ToUpper(metadata.Name) == BUSY_COIN_SYMBOL {
		response.Message = "Symbol/TokenName cannot be BUSY!"
		logger.Error(response.Message)
		return response, generateError(412, "MIN007", response.Message)
	}

	properties, converted := metadata.Properties.(map[string]interface{})
	if converted {
		for key, value := range properties {
			val, ok := value.(string)
			if ok && val == "" {
				response.Message = fmt.Sprintf("%s should not be empty in properties", key)
				logger.Error(response.Message)
				return response, generateError(400, "MIN008", response.Message)
			}
		}
	}

	if metadata.Logo == "" || metadata.Name == "" || metadata.Type == "" {
		response.Message = "Invalid Metadata"
		logger.Error(response.Message)
		return response, generateError(412, "MIN009", response.Message)
	}

	if !contains([]string{"NFT", "GAME"}, metadata.Type) {
		response.Message = "Only NFT and GAME are supported as type in metadata"
		logger.Error(response.Message)
		return response, generateError(412, "MIN010", response.Message)
	}
	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(account)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching account %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN011", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", account)
		logger.Error(response.Message)
		return response, generateError(404, "MIN012", response.Message)
	}

	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN013", response.Message)
	}
	operator, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN014", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, account, BUSY_COIN_SYMBOL)
	mintFeeString, _ := getTokenIssueFeeForTokenType(ctx, metadata.Type)
	mintFee, _ := new(big.Int).SetString(mintFeeString, 10)
	if balance.Cmp(mintFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to mint new tokens", account)
		logger.Error(response.Message)
		return response, generateError(402, "MIN015", response.Message)
	}
	err = txFeeHelper(ctx, account, BUSY_COIN_SYMBOL, mintFee.String(), "mint")
	if err != nil {
		response.Message = "Error while burning mint transaction fee"
		logger.Error(response.Message)
		return response, generateError(500, "MIN016", response.Message)
	}
	bigAmount, _ := new(big.Int).SetString(totalSupply, 10)
	// Mint tokens
	err = mintHelper(ctx, operator, account, symbol, bigAmount)
	if err != nil {
		response.Message = fmt.Sprintf("Error while minting the tokens: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN017", response.Message)
	}

	busyTokensInfo := BusyTokensInfo{
		Account:      account,
		TokenAddress: tokenAddress,
		CreatedAT:    time.Now(),
		MetaData:     metadata,
		TotalSupply:  bigAmount.String(),
		TokenSymbol:  symbol,
	}
	// putting the tokenMetaData
	busyTokensInfoAsBytes, _ = json.Marshal(busyTokensInfo)
	err = ctx.GetStub().PutState(tokenAddress, busyTokensInfoAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN018", response.Message)
	}
	// sending the balance event
	nftEventData := NFTEvent{
		UserAddresses: UserAddress{
			Address: account,
			Token:   BUSY_COIN_SYMBOL,
		},
		NFTList: []NFTEventInfo{
			{
				Account:   account,
				TokenType: metadata.Type,
				Symbol:    symbol,
			},
		},
		TransactionFee: mintFee.String(),
		TransactionId:  response.TxID,
	}
	nftEventDataAsBytes, _ := json.Marshal(nftEventData)
	err = ctx.GetStub().SetEvent(NFT_EVENT, nftEventDataAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the NFT event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NFTB001", response.Message)
	}

	err = addTotalSupplyTokensUTXO(ctx, symbol, bigAmount)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN019", response.Message)
	}

	// Send Success response
	transferSingleData := TransferSingle{operator, "0x", account, symbol, bigAmount.String(), tokenAddress}
	response.Data = transferSingleData
	response.Success = true
	response.Message = "Tokens has been successfully minted"
	return response, nil
}

// MintBatch creates amount tokens for each token type and assigns them to account.
func (s *BusyTokens) MintBatch(ctx contractapi.TransactionContextInterface, account string, symbols []string, totalSupplies []string, metaDataBatch *MetaDataBatch) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if len(symbols) != len(totalSupplies) || len(symbols) != len(metaDataBatch.MetaData) {
		return nil, generateError(400, "MIN020", "ids ,amounts and must have the same length")
	}

	visited := make(map[string]bool, len(symbols))
	for _, symbol := range symbols {
		if visited[symbol] {
			return nil, generateError(400, "MIN022", "duplicates not allowed in symbols")
		}
		visited[symbol] = true
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	// checking if the token already exists
	walletAsBytes, err := ctx.GetStub().GetState(account)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching account %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN011", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", account)
		logger.Error(response.Message)
		return response, generateError(500, "MIN012", response.Message)
	}

	tokenAddresses := []string{}
	nftList := []NFTEventInfo{}

	mintFeeBatch := new(big.Int).SetUint64(0)
	for idx, symbol := range symbols {
		exist, err := ifTokenExists(ctx, symbol)
		if err != nil {
			response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "MIN005", response.Message)
		}
		if exist {
			response.Message = fmt.Sprintf("Token with same symbol %s already exists", symbol)
			logger.Error(response.Message)
			return response, generateError(409, "MIN006", response.Message)
		}

		tokenAddress := generateTokenAddress(symbol)
		tokenAddresses = append(tokenAddresses, tokenAddress)
		busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "MIN001", response.Message)
		}

		if busyTokensInfoAsBytes != nil {
			response.Message = "Token already exists"
			logger.Info(response.Message)
			return response, generateError(409, "MIN002", response.Message)
		}
		// putting the tokenMetaData into the state

		if metaDataBatch.MetaData[idx].Logo == "" || metaDataBatch.MetaData[idx].Name == "" || metaDataBatch.MetaData[idx].Type == "" {
			response.Message = "Invalid Metadata"
			logger.Error(response.Message)
			return response, generateError(412, "MIN009", response.Message)
		}

		if !contains([]string{"NFT", "GAME"}, metaDataBatch.MetaData[idx].Type) {
			response.Message = "Only NFT and GAME are supported as type in metadata"
			logger.Error(response.Message)
			return response, generateError(412, "MIN010", response.Message)
		}

		if strings.ToUpper(symbol) == BUSY_COIN_SYMBOL || strings.ToUpper(metaDataBatch.MetaData[idx].Name) == BUSY_COIN_SYMBOL {
			response.Message = "Symbol/TokenName cannot be BUSY!"
			logger.Error(response.Message)
			return response, generateError(412, "MIN007", response.Message)
		}

		properties, converted := metaDataBatch.MetaData[idx].Properties.(map[string]interface{})
		if converted {
			for key, value := range properties {
				val, ok := value.(string)
				if ok && val == "" {
					response.Message = fmt.Sprintf("%s should not be empty in properties", key)
					logger.Error(response.Message)
					return response, generateError(400, "MIN008", response.Message)
				}
			}
		}
		exp := regexp.MustCompile(`^[\w]+([-\s]{1}[\w]+)*$`)
		oneLetter := regexp.MustCompile(`.*.[a-zA-Z].*$`)
		tokenLength := regexp.MustCompile(`^.{3,20}$`)
		// checking for token Name
		if !exp.MatchString(metaDataBatch.MetaData[idx].Name) || !tokenLength.MatchString(metaDataBatch.MetaData[idx].Name) || !oneLetter.MatchString(metaDataBatch.MetaData[idx].Name) {
			response.Message = "Invalid token name"
			logger.Error(response.Message)
			return response, generateError(412, "MIN003", response.Message)
		}

		symbolLength := regexp.MustCompile(`^.{3,5}$`)
		// checking for tokenSymbol
		if !exp.MatchString(symbol) || !symbolLength.MatchString(symbol) || !oneLetter.MatchString(symbol) {
			response.Message = "Invalid token symbol"
			logger.Error(response.Message)
			return response, generateError(412, "MIN004", response.Message)
		}

		bigAmount, _ := new(big.Int).SetString(totalSupplies[idx], 10)

		busyTokensInfo := BusyTokensInfo{
			Account:      account,
			CreatedAT:    time.Now(),
			TokenAddress: tokenAddress,
			MetaData:     metaDataBatch.MetaData[idx],
			TotalSupply:  bigAmount.String(),
			TokenSymbol:  symbol,
		}
		busyTokensInfoAsBytes, _ = json.Marshal(busyTokensInfo)
		err = ctx.GetStub().PutState(tokenAddress, busyTokensInfoAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "MIN018", response.Message)
		}
		err = addTotalSupplyTokensUTXO(ctx, symbol, bigAmount)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "MIN019", response.Message)
		}

		mintFeeString, _ := getTokenIssueFeeForTokenType(ctx, metaDataBatch.MetaData[idx].Type)
		mintFee, _ := new(big.Int).SetString(mintFeeString, 10)
		mintFeeBatch = mintFeeBatch.Add(mintFeeBatch, mintFee)

		nftList = append(nftList, NFTEventInfo{
			Account:   account,
			TokenType: metaDataBatch.MetaData[idx].Type,
			Symbol:    symbol,
		})
	}
	// Get Common Name of submitting client
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN013", response.Message)
	}
	operator, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MIN014", response.Message)
	}
	amountToSend := make(map[string]*big.Int) // token symbol => amount

	for i := 0; i < len(totalSupplies); i++ {
		amount, _ := new(big.Int).SetString(totalSupplies[i], 10)
		if _, ok := amountToSend[symbols[i]]; !ok {
			amountToSend[symbols[i]] = amount
		} else {
			amountToSend[symbols[i]] = amountToSend[symbols[i]].Add(amountToSend[symbols[i]], amount)
		}
	}
	// Copy the map keys and sort it. This is necessary because iterating maps in Go is not deterministic
	amountToSendKeys := sortedKeys(amountToSend)
	// Mint tokens
	for _, id := range amountToSendKeys {
		bigAmount := amountToSend[id]
		err = mintHelper(ctx, operator, account, id, bigAmount)
		if err != nil {
			response.Message = fmt.Sprintf("Error while minting the batch %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "MIN021", response.Message)
		}
	}
	balance, _ := getBalanceHelper(ctx, account, BUSY_COIN_SYMBOL)
	if balance.Cmp(mintFeeBatch) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to mint enw tokens", account)
		logger.Error(response.Message)
		return response, generateError(402, "MIN015", response.Message)
	}
	err = txFeeHelper(ctx, account, BUSY_COIN_SYMBOL, mintFeeBatch.String(), "mintGame")
	if err != nil {
		response.Message = "Error while burning mint Transaction Fee"
		logger.Error(response.Message)
		return response, generateError(500, "MIN016", response.Message)
	}

	// sending the balance event
	nftEventData := NFTEvent{
		UserAddresses: UserAddress{
			Address: account,
			Token:   BUSY_COIN_SYMBOL,
		},
		NFTList:        nftList,
		TransactionFee: mintFeeBatch.String(),
		TransactionId:  response.TxID,
	}
	nftEventDataAsBytes, _ := json.Marshal(nftEventData)
	err = ctx.GetStub().SetEvent(NFT_EVENT, nftEventDataAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the NFT event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NFTB001", response.Message)
	}
	// Emit TransferBatch event
	transferBatchData := TransferBatch{operator, "0x", account, symbols, totalSupplies, tokenAddresses}
	response.Data = transferBatchData
	response.Message = "Request to mint the tokens has been successfully accepted"
	response.Success = true
	return response, nil
}

// BurnBatch destroys amount tokens of for each token type from account.
func (s *BusyTokens) BurnBatch(ctx contractapi.TransactionContextInterface, account string, symbols []string, amounts []string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	if account == "0x" {
		return nil, generateError(400, "BRNB001", "Burn to the zero address")
	}

	if len(symbols) != len(amounts) {
		return nil, generateError(412, "BRNB002", "ids and amounts must have the same length")
	}

	if isDuplicate(symbols) {
		return nil, generateError(412, "BRNB003", "Duplicates are not allowed in symbols")
	}
	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BRNB004", response.Message)
	}
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BRNB005", response.Message)
	}

	if defaultWalletAddress != account {
		response.Message = fmt.Sprintf("Wallet Id does not match with %s", defaultWalletAddress)
		logger.Error(response.Message)
		return response, generateError(409, "BRNB006", response.Message)
	}
	tokenAddresses := []string{}
	nftList := []NFTEventInfo{}
	bigAmounts := make([]*big.Int, len(symbols))
	for idx, symbol := range symbols {
		// checking if the token already exists

		tokenAddress := generateTokenAddress(symbol)
		tokenAddresses = append(tokenAddresses, tokenAddress)
		busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BRNB007", response.Message)
		}
		if busyTokensInfoAsBytes == nil {
			response.Message = fmt.Sprintf("Token %s does not exist", symbol)
			logger.Info(response.Message)
			return response, generateError(404, "BRNB008", response.Message)
		}

		busyTokensInfo := BusyTokensInfo{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BRNB009", response.Message)
		}

		if defaultWalletAddress != busyTokensInfo.Account {
			response.Message = "Only owner can burn the tokens"
			logger.Error(response.Message)
			return response, generateError(403, "BRNB010", response.Message)
		}
		bigAmount, _ := new(big.Int).SetString(amounts[idx], 10)
		bigAmounts[idx] = bigAmount
		err = addTotalSupplyTokensUTXO(ctx, symbol, new(big.Int).Set(bigAmount).Mul(minusOne, bigAmount))
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BRNB011", response.Message)
		}

		nftList = append(nftList, NFTEventInfo{
			Account:   account,
			Symbol:    symbol,
			TokenType: busyTokensInfo.MetaData.Type,
		})

	}
	walletAsBytes, err := ctx.GetStub().GetState(account)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BRNB012", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", account)
		logger.Error(response.Message)
		return response, generateError(404, "BRNB013", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, account, BUSY_COIN_SYMBOL)
	burnFeeString, _ := getCurrentTxFee(ctx)
	burnFee, _ := new(big.Int).SetString(burnFeeString, 10)
	numberofTokens := new(big.Int).SetInt64(int64(len(symbols)))
	burnBatchFee := new(big.Int).Mul(burnFee, numberofTokens)
	if balance.Cmp(burnBatchFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to burn tokens", account)
		logger.Error(response.Message)
		return response, generateError(402, "BRNB014", response.Message)
	}

	err = removeBalance(ctx, account, symbols, bigAmounts)
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning the tokens: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BRNB015", response.Message)
	}

	err = txFeeHelper(ctx, account, BUSY_COIN_SYMBOL, burnBatchFee.String(), "mintGame")
	if err != nil {
		response.Message = "Error while burning mint transaction fee"
		logger.Error(response.Message)
		return response, generateError(500, "BRNB016", response.Message)
	}

	// sending the balance event
	nftEventData := NFTEvent{
		UserAddresses: UserAddress{
			Address: account,
			Token:   BUSY_COIN_SYMBOL,
		},
		NFTList:        nftList,
		TransactionFee: burnBatchFee.String(),
		TransactionId:  response.TxID,
	}
	nftEventDataAsBytes, _ := json.Marshal(nftEventData)
	err = ctx.GetStub().SetEvent(NFT_EVENT, nftEventDataAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the NFT event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NFTB001", response.Message)
	}

	burnBatchData := TransferBatch{account, account, "0x", symbols, amounts, tokenAddresses}
	response.Data = burnBatchData
	response.Message = "Tokens burn successfully"
	response.Success = true
	return response, nil
}

// TransferFrom transfers tokens from sender account to recipient account
// recipient account must be a valid clientID as returned by the ClientID() function
// This function triggers a TransferSingle event
func (s *BusyTokens) TransferFrom(ctx contractapi.TransactionContextInterface, sender string, recipient string, symbol string, amount string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	if sender == recipient {
		return nil, generateError(400, "NTRA001", "Transfer to self")
	}

	if recipient == "0x" {
		return nil, generateError(400, "NTRA002", "Transfer to the zero address")
	}
	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	// checking if the token already exists
	tokenAddress := generateTokenAddress(symbol)
	busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA003", response.Message)
	}
	if busyTokensInfoAsBytes == nil {
		response.Message = "Token does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "NTRA004", response.Message)
	}

	busyTokensInfo := BusyTokensInfo{}
	err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA005", response.Message)
	}

	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA006", response.Message)
	}
	operator, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA007", response.Message)
	}
	bigAmount, _ := new(big.Int).SetString(amount, 10)
	if bigAmount.Cmp(bigZero) == 0 {
		response.Message = "Amount cannot be zero"
		logger.Error(response.Message)
		return response, generateError(412, "NTRA008", response.Message)
	}

	// Check whether operator is owner or approved
	if operator != sender {
		approved, err := _isApprovedForAll(ctx, sender, operator)
		if err != nil {
			response.Message = fmt.Sprintf("Failed to get the approval status of operator: %v", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NTRA009", response.Message)
		}
		if !approved {
			response.Message = "Caller is neither the owner nor is approved"
			logger.Error(response.Message)
			return response, generateError(403, "NTRA010", response.Message)
		}
	}

	balance, _ := getBalanceHelper(ctx, sender, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	transferFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(transferFee) == -1 {
		response.Message = fmt.Sprintf("You %s does not have the enough balance to tranfer tokens", sender)
		logger.Error(response.Message)
		return response, generateError(402, "NTRA011", response.Message)
	}
	err = txFeeHelper(ctx, sender, BUSY_COIN_SYMBOL, transferFee.String(), "transfer")
	if err != nil {
		response.Message = "Error while burning transaction fee"
		logger.Error(response.Message)
		return response, generateError(500, "NTRA012", response.Message)
	}

	// Withdraw the funds from the sender address
	err = removeBalance(ctx, sender, []string{symbol}, []*big.Int{bigAmount})
	if err != nil {
		response.Message = fmt.Sprintf("Error while removing balance %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA013", response.Message)
	}

	// Deposit the fund to the recipient address
	err = addBalance(ctx, sender, recipient, symbol, bigAmount)
	if err != nil {
		response.Message = "Error while adding balance to the recipient"
		logger.Error(response.Message)
		return response, generateError(500, "NTRA014", response.Message)
	}

	// sending the balance event
	nftEventData := NFTEvent{
		UserAddresses: UserAddress{
			Address: sender,
			Token:   BUSY_COIN_SYMBOL,
		},
		NFTList: []NFTEventInfo{
			{
				Account:   sender,
				Symbol:    symbol,
				TokenType: busyTokensInfo.MetaData.Type,
			},
			{
				Account:   recipient,
				Symbol:    symbol,
				TokenType: busyTokensInfo.MetaData.Type,
			},
		},
		TransactionFee: transferFee.String(),
		TransactionId:  response.TxID,
	}
	nftEventDataAsBytes, _ := json.Marshal(nftEventData)
	err = ctx.GetStub().SetEvent(NFT_EVENT, nftEventDataAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the NFT event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NFTB001", response.Message)
	}

	transferSingleData := TransferSingle{operator, sender, recipient, symbol, bigAmount.String(), tokenAddress}
	response.Data = transferSingleData
	response.Message = "Request to transfer tokens has been successfully accepted"
	response.Success = true
	return response, nil
}

// BatchTransferFrom transfers multiple tokens from sender account to recipient account
// This function triggers a TransferBatch event
func (s *BusyTokens) BatchTransferFrom(ctx contractapi.TransactionContextInterface, sender string, recipient string, symbols []string, amounts []string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	if sender == recipient {
		return nil, generateError(400, "NTRA001", "Transfer to self")
	}

	if len(symbols) != len(amounts) {
		return nil, generateError(412, "NTRA015", "ids and amounts must have the same length")
	}
	if recipient == "0x" {
		return nil, generateError(400, "NTRA002", "Transfer to the zero address")
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA006", response.Message)
	}
	operator, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NTRA007", response.Message)
	}

	// Check whether operator is owner or approved
	if operator != sender {
		approved, err := _isApprovedForAll(ctx, sender, operator)
		if err != nil {
			response.Message = fmt.Sprintf("Failed to get the approval for operator: %v", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NTRA009", response.Message)
		}
		if !approved {
			response.Message = "Caller is not owner or is approved"
			logger.Error(response.Message)
			return response, generateError(403, "NTRA010", response.Message)
		}
	}

	tokenAddresses := []string{}
	nftList := []NFTEventInfo{}
	bigAmounts := make([]*big.Int, len(symbols))
	for idx := range symbols {
		// checking if the token already exists
		tokenAddress := generateTokenAddress(symbols[idx])
		tokenAddresses = append(tokenAddresses, tokenAddress)
		busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NTRA003", response.Message)
		}
		if busyTokensInfoAsBytes == nil {
			response.Message = "Token does not exist"
			logger.Info(response.Message)
			return response, generateError(404, "NTRA004", response.Message)
		}

		busyTokensInfo := BusyTokensInfo{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NTRA005", response.Message)
		}

		nftList = append(nftList, NFTEventInfo{
			Account:   sender,
			TokenType: busyTokensInfo.MetaData.Type,
			Symbol:    symbols[idx],
		})
		nftList = append(nftList, NFTEventInfo{
			Account:   recipient,
			TokenType: busyTokensInfo.MetaData.Type,
			Symbol:    symbols[idx],
		})
		bigAmount, _ := new(big.Int).SetString(amounts[idx], 10)
		bigAmounts[idx] = bigAmount
		if bigAmount.Cmp(bigZero) == 0 {
			response.Message = "Amount cannot be zero"
			logger.Error(response.Message)
			return response, generateError(412, "NTRA008", response.Message)
		}

	}

	balance, _ := getBalanceHelper(ctx, sender, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	transferFee, _ := new(big.Int).SetString(txFee, 10)
	numberofTokens := new(big.Int).SetInt64(int64(len(symbols)))
	transferFeeBatch := new(big.Int).Mul(transferFee, numberofTokens)
	if balance.Cmp(transferFeeBatch) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to tranfer tokens", sender)
		logger.Error(response.Message)
		return response, generateError(402, "NTRA011", response.Message)
	}
	err = txFeeHelper(ctx, sender, BUSY_COIN_SYMBOL, transferFeeBatch.String(), "transferBatch")
	if err != nil {
		response.Message = "error while burning Transaction Fee"
		logger.Error(response.Message)
		return response, generateError(500, "NTRA012", response.Message)
	}

	// Withdraw the funds from the sender address
	err = removeBalance(ctx, sender, symbols, bigAmounts)
	if err != nil {
		response.Message = fmt.Sprintf("Error while removing the balance %s", err.Error())
		logger.Error(response.Message)
		return nil, generateError(500, "NTRA013", response.Message)
	}

	// Group amount by token symbols because we can only send token to a recipient only one time in a block. This prevents key conflicts
	amountToSend := make(map[string]*big.Int) // token symbol => amount

	for i := 0; i < len(amounts); i++ {
		amount, _ := new(big.Int).SetString(amounts[i], 10)
		if _, ok := amountToSend[symbols[i]]; !ok {
			amountToSend[symbols[i]] = amount
		} else {
			amountToSend[symbols[i]] = amountToSend[symbols[i]].Add(amountToSend[symbols[i]], amount)
		}
	}

	// Copy the map keys and sort it. This is necessary because iterating maps in Go is not deterministic
	amountToSendKeys := sortedKeys(amountToSend)

	// Deposit the funds to the recipient address
	for _, id := range amountToSendKeys {
		amount := amountToSend[id]
		err = addBalance(ctx, sender, recipient, id, amount)
		if err != nil {
			response.Message = fmt.Sprintf("Error while adding the balance to the recipient %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NTRA014", response.Message)
		}
	}

	// sending the balance event
	nftEventData := NFTEvent{
		UserAddresses: UserAddress{
			Address: sender,
			Token:   BUSY_COIN_SYMBOL,
		},
		NFTList:        nftList,
		TransactionFee: transferFeeBatch.String(),
		TransactionId:  response.TxID,
	}
	nftEventDataAsBytes, _ := json.Marshal(nftEventData)
	err = ctx.GetStub().SetEvent(NFT_EVENT, nftEventDataAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the NFT event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NFTB001", response.Message)
	}

	transferBatchData := TransferBatch{operator, sender, recipient, symbols, amounts, tokenAddresses}
	response.Data = transferBatchData
	response.Message = "Request to transfer tokens has been successfully accepted"
	response.Success = true
	return response, nil
}

// IsApprovedForAll returns true if operator is approved to transfer account's tokens.
func (s *BusyTokens) IsApprovedForAll(ctx contractapi.TransactionContextInterface, account string, operator string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// check if operator does not exists
	walletAsBytes, err := ctx.GetStub().GetState(operator)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "GAPL001", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Operator %s does not exist", operator)
		logger.Error(response.Message)
		return response, generateError(404, "GAPL002", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	isApproved, err := _isApprovedForAll(ctx, account, operator)
	if err != nil {
		return response, generateError(500, "GAPL003", err.Error())
	}
	response.Data = isApproved
	response.Message = "The operator's approval status has been successfully fetched"
	response.Success = true
	return response, nil
}

// _isApprovedForAll returns true if operator is approved to transfer account's tokens.
func _isApprovedForAll(ctx contractapi.TransactionContextInterface, account string, operator string) (bool, error) {
	approvalKey, err := ctx.GetStub().CreateCompositeKey(approvalPrefix, []string{account, operator})
	if err != nil {
		return false, fmt.Errorf("failed to create the composite key for prefix %s: %v", approvalPrefix, err)
	}

	approvalBytes, err := ctx.GetStub().GetState(approvalKey)
	if err != nil {
		return false, fmt.Errorf("failed to read approval of operator %s for account %s from world state: %v", operator, account, err)
	}

	if approvalBytes == nil {
		return false, nil
	}

	var approved bool
	err = json.Unmarshal(approvalBytes, &approved)
	if err != nil {
		return false, fmt.Errorf("failed to decode approval JSON of operator %s for account %s: %v", operator, account, err)
	}

	return approved, nil
}

// SetApprovalForAll returns true if operator is approved to transfer account's tokens.
func (s *BusyTokens) SetApprovalForAll(ctx contractapi.TransactionContextInterface, operator string, approved bool) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SAPL001", response.Message)
	}
	account, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SAPL002", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	if account == operator {
		return nil, generateError(409, "SAPL003", "setting approval status for self")
	}

	// check if operator does not exists
	walletAsBytes, err := ctx.GetStub().GetState(operator)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching operator wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SAPL004", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Operator %s does not exist", operator)
		logger.Error(response.Message)
		return response, generateError(404, "SAPL005", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, account, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(bigTxFee) == -1 {
		response.Message = fmt.Sprintf("You %s do not have enough balance to set the approval for NFT/GAME tokens", account)
		logger.Error(response.Message)
		return response, generateError(402, "SAPL006", response.Message)
	}
	err = txFeeHelper(ctx, account, BUSY_COIN_SYMBOL, bigTxFee.String(), "busynftTransfer")
	if err != nil {
		response.Message = "Error while burning Transaction Fee"
		logger.Error(response.Message)
		return response, generateError(500, "SAPL007", response.Message)
	}

	approvalKey, err := ctx.GetStub().CreateCompositeKey(approvalPrefix, []string{account, operator})
	if err != nil {
		response.Message = fmt.Sprintf("failed to create the composite key for prefix %s: %v", approvalPrefix, err)
		logger.Error(response.Message)
		return response, generateError(500, "SAPL008", response.Message)
	}

	approvalJSON, err := json.Marshal(approved)
	if err != nil {
		response.Message = fmt.Sprintf("failed to encode approval of operator %s for account %s: %v", operator, account, err)
		logger.Error(response.Message)
		return response, generateError(500, "SAPL009", response.Message)
	}

	err = ctx.GetStub().PutState(approvalKey, approvalJSON)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SAPL010", response.Message)
	}

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
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	approvalForAllData := ApprovalForAll{account, operator, approved}
	response.Data = approvalForAllData
	response.Message = "Request to set approval has been successfully accepted"
	response.Success = true
	return response, nil
}

// BalanceOf returns the balance of the given account
func (s *BusyTokens) BalanceOf(ctx contractapi.TransactionContextInterface, account string, symbol string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// checking if the token already exists
	tokenAddress := generateTokenAddress(symbol)
	metaDateAsBytes, err := ctx.GetStub().GetState(tokenAddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NBAL001", response.Message)
	}
	if metaDateAsBytes == nil {
		response.Message = fmt.Sprintf("Token %s does not exist", symbol)
		logger.Info(response.Message)
		return response, generateError(404, "NBAL002", response.Message)
	}
	balance, err := balanceOfHelper(ctx, account, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching the balance %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "NBAL003", response.Message)
	}
	response.Data = balance
	response.Message = "Balance of the token has been successfully fetched"
	response.Success = true
	return response, nil
}

// BalanceOfBatch returns the balance of multiple account/token pairs
func (s *BusyTokens) BalanceOfBatch(ctx contractapi.TransactionContextInterface, accounts []string, symbols []string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	if len(accounts) != len(symbols) {
		return nil, generateError(412, "NBAL006", "accounts and ids must have the same length")
	}

	balances := make([]string, len(accounts))

	for i := 0; i < len(accounts); i++ {
		var err error
		// checking if the token already exists
		tokenAddress := generateTokenAddress(symbols[i])
		metaDateAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NBAL001", response.Message)
		}
		if metaDateAsBytes == nil {
			response.Message = fmt.Sprintf("Token %s does not exist", symbols[i])
			logger.Info(response.Message)
			return response, generateError(404, "NBAL002", response.Message)
		}

		// check if wallet already exists
		walletAsBytes, err := ctx.GetStub().GetState(accounts[i])
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NBAL005", response.Message)
		}
		if walletAsBytes == nil {
			response.Message = fmt.Sprintf("Account %s does not exist", accounts[i])
			logger.Error(response.Message)
			return response, generateError(404, "NBAL004", response.Message)
		}
		balances[i], err = balanceOfHelper(ctx, accounts[i], symbols[i])
		if err != nil {
			response.Message = fmt.Sprintf("Error while fetching the balance %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "NBAL003", response.Message)
		}
	}

	response.Data = balances
	response.Message = "Balance of the tokens has been successfully fetched"
	response.Success = true
	return response, nil
}

// GetTokenInfo returns the metadata, owner and tokenAddress of the Requested token
func (s *BusyTokens) GetTokenInfo(ctx contractapi.TransactionContextInterface, symbol string, tokenType string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if tokenType != "GAME" && tokenType != "NFT" && tokenType != "BUSY20" {
		response.Message = fmt.Sprintf("unsupported token type: %s", tokenType)
		logger.Error(response.Message)
		return response, generateError(412, "GTIN001", response.Message)
	}
	if tokenType == "BUSY20" {
		tokenAddress := generateTokenStateAddress(symbol)
		busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN002", response.Message)
		}
		if busyTokensInfoAsBytes == nil {
			response.Message = "Token does not exist"
			logger.Info(response.Message)
			return response, generateError(404, "GTIN003", response.Message)
		}

		busyTokensInfo := Token{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN004", response.Message)
		}
		totalSupply, _, err := pruneUTXOs(ctx, TOTAL_SUPPLY_KEY, symbol)
		if err != nil {
			response.Message = fmt.Sprintf("Error while pruning utxo: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN005", response.Message)
		}
		busyTokensInfo.TotalSupply = totalSupply.String()
		response.Data = busyTokensInfo

	} else {
		tokenAddress := generateTokenAddress(symbol)
		busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN002", response.Message)
		}
		if busyTokensInfoAsBytes == nil {
			response.Message = "Token does not exist"
			logger.Info(response.Message)
			return response, generateError(404, "GTIN003", response.Message)
		}

		busyTokensInfo := BusyTokensInfo{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN004", response.Message)
		}

		if tokenType != busyTokensInfo.MetaData.Type {
			response.Message = "Token does not exist"
			logger.Info(response.Message)
			return response, generateError(404, "GTIN003", response.Message)
		}

		totalSupply, _, err := pruneUTXOs(ctx, TOTAL_SUPPLY_KEY_NFT, symbol)
		if err != nil {
			response.Message = fmt.Sprintf("Error while pruning utxo: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "GTIN005", response.Message)
		}

		busyTokensInfo.TotalSupply = totalSupply.String()
		response.Data = busyTokensInfo

	}
	// putting the tokenMetaData
	response.Message = "The tokens Info has been successfully fetched"
	response.Success = true
	return response, nil
}

// UpdateTokenMetaData to update TokenMetaData
func (s *BusyTokens) UpdateTokenMetaData(ctx contractapi.TransactionContextInterface, symbol string, metadata TokenMetaData, tokenType string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if tokenType != "BUSY20" && tokenType != "NFT" && tokenType != "GAME" {
		response.Message = fmt.Sprintf("tokenType %s not supported", tokenType)
		logger.Error(response.Message)
		return response, generateError(412, "UTMD001", response.Message)
	}
	// getting the tokenMetaData
	tokenAddress := generateTokenAddress(symbol)

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	// checking if tokenType to BUSY20
	if tokenType == "BUSY20" {
		tokenAddress = generateTokenStateAddress(symbol)
	}
	busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTMD002", response.Message)
	}
	if busyTokensInfoAsBytes == nil {
		response.Message = "Token does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "UTMD003", response.Message)
	}

	if metadata.Logo == "" {
		response.Message = "Logo cannot be empty"
		logger.Info(response.Message)
		return response, generateError(412, "UTMD004", response.Message)
	}
	// Get Common Name of submitting client identity
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("failed to get Common name: %v", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTMD005", response.Message)
	}
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTMD006", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if balance.Cmp(bigTxFee) == -1 {
		response.Message = fmt.Sprintf("User %s does not have the enough balance to Update Metadata of NFT", defaultWalletAddress)
		logger.Error(response.Message)
		return response, generateError(412, "UTMD007", response.Message)
	}
	err = txFeeHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL, bigTxFee.String(), "busynftTransfer")
	if err != nil {
		response.Message = "Error while burning Transaction Fee"
		logger.Error(response.Message)
		return response, generateError(500, "UTMD008", response.Message)
	}

	if tokenType == "BUSY20" {
		busytwentyTokensInfo := Token{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busytwentyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "UTMD009", response.Message)
		}
		if busytwentyTokensInfo.MetaData.Type != tokenType || busytwentyTokensInfo.MetaData.Type != metadata.Type {
			response.Message = "Token Type cannot be updated"
			logger.Error(response.Message)
			return response, generateError(412, "UTMD010", response.Message)
		}
		busytwentyTokensInfo.MetaData = metadata

		// unmarshall and putting in state
		busyTokensInfoAsBytes, _ = json.Marshal(busytwentyTokensInfo)
		err = ctx.GetStub().PutState(tokenAddress, busyTokensInfoAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "UTMD011", response.Message)
		}
		response.Data = busytwentyTokensInfo
	} else {
		busyTokensInfo := BusyTokensInfo{}
		err = json.Unmarshal(busyTokensInfoAsBytes, &busyTokensInfo)
		if err != nil {
			response.Message = fmt.Sprintf("Error while Marshalling the data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "UTMD009", response.Message)
		}

		if defaultWalletAddress != busyTokensInfo.Account {
			response.Message = fmt.Sprintf("The account %s is not the owner of %s", defaultWalletAddress, symbol)
			logger.Error(response.Message)
			return response, generateError(403, "UTMD012", response.Message)
		}
		if busyTokensInfo.MetaData.Type != metadata.Type || busyTokensInfo.MetaData.Type != tokenType {
			response.Message = "Token Type cannot be updated"
			logger.Error(response.Message)
			return response, generateError(412, "UTMD010", response.Message)
		}
		busyTokensInfo.MetaData = metadata

		// unmarshall and putting in state
		busyTokensInfoAsBytes, _ = json.Marshal(busyTokensInfo)
		err = ctx.GetStub().PutState(tokenAddress, busyTokensInfoAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "UTMD011", response.Message)

		}
		response.Data = busyTokensInfo
	}

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
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "The token's metadata has been successfully updated"
	response.Success = true
	return response, nil
}
func mintHelper(ctx contractapi.TransactionContextInterface, operator string, account string, symbol string, bigAmount *big.Int) error {
	if account == "0x" {
		return fmt.Errorf("mint to the zero address")
	}

	err := addBalance(ctx, operator, account, symbol, bigAmount)
	if err != nil {
		return err
	}

	return nil
}

func addBalance(ctx contractapi.TransactionContextInterface, sender string, recipient string, symbol string, bigAmount *big.Int) error {

	balanceKey, err := ctx.GetStub().CreateCompositeKey(balancePrefix, []string{recipient, symbol, sender})
	if err != nil {
		return fmt.Errorf("failed to create the composite key for prefix %s: %v", balancePrefix, err)
	}

	balanceBytes, err := ctx.GetStub().GetState(balanceKey)
	if err != nil {
		return fmt.Errorf("failed to read account %s from world state: %v", recipient, err)
	}
	if balanceBytes != nil {
		balance, _ := new(big.Int).SetString(string(balanceBytes), 10)
		bigAmount = bigAmount.Add(balance, bigAmount)
	}

	err = ctx.GetStub().PutState(balanceKey, []byte(bigAmount.String()))
	if err != nil {
		return err
	}

	return nil
}

func setBalance(ctx contractapi.TransactionContextInterface, sender string, recipient string, symbol string, bigAmount *big.Int) error {

	balanceKey, err := ctx.GetStub().CreateCompositeKey(balancePrefix, []string{recipient, symbol, sender})
	if err != nil {
		return fmt.Errorf("failed to create the composite key for prefix %s: %v", balancePrefix, err)
	}

	err = ctx.GetStub().PutState(balanceKey, []byte(bigAmount.String()))
	if err != nil {
		return err
	}

	return nil
}

// addTotalSupplyTokensUTXO add utxo in total supply for nft token
func addTotalSupplyTokensUTXO(ctx contractapi.TransactionContextInterface, tokenSymbol string, amount *big.Int) error {
	err := addUTXO(ctx, TOTAL_SUPPLY_KEY_NFT, amount, tokenSymbol)
	if err != nil {
		return err
	}
	return nil
}

// GetTotalSupply get total supply of specified token
func (s *BusyTokens) GetTotalSupplyNftBatch(ctx contractapi.TransactionContextInterface, symbols []string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	totalSupplies := []string{}

	for _, symbol := range symbols {
		if symbol == "" {
			symbol = BUSY_COIN_SYMBOL
		}
		var token Token
		tokenAsBytes, err := ctx.GetStub().GetState(generateTokenAddress(symbol))
		if err != nil {
			response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
			logger.Error(response.Message)
			return response, fmt.Errorf(response.Message)
		}
		if tokenAsBytes == nil {
			response.Message = fmt.Sprintf("Symbol %s does not exist", symbol)
			logger.Error(response.Message)
			return response, fmt.Errorf(response.Message)
		}
		_ = json.Unmarshal(tokenAsBytes, &token)

		totalSupply, _, err := pruneUTXOs(ctx, TOTAL_SUPPLY_KEY_NFT, symbol)
		if err != nil {
			response.Message = fmt.Sprintf("Error while prunning utxo: %s", err.Error())
			logger.Error(response.Message)
			return response, fmt.Errorf(response.Message)
		}
		token.TotalSupply = totalSupply.String()
		totalSupplies = append(totalSupplies, fmt.Sprintf("%s %s", totalSupply.String(), symbol))

	}
	response.Message = "Total supply has been successfully fetched"
	response.Data = totalSupplies
	response.Success = true
	return response, nil
}

func removeBalance(ctx contractapi.TransactionContextInterface, sender string, symbols []string, amounts []*big.Int) error {
	// Calculate the total amount of each token to withdraw
	necessaryFunds := make(map[string]*big.Int) // token symbol -> necessary amount

	for i := 0; i < len(amounts); i++ {
		if _, ok := necessaryFunds[symbols[i]]; !ok {
			necessaryFunds[symbols[i]] = new(big.Int).Set((amounts[i]))
		} else {
			necessaryFunds[symbols[i]] = necessaryFunds[symbols[i]].Add(necessaryFunds[symbols[i]], amounts[i])
		}

	}

	// Copy the map keys and sort it. This is necessary because iterating maps in Go is not deterministic
	necessaryFundsKeys := sortedKeys(necessaryFunds)

	// Check whether the sender has the necessary funds and withdraw them from the account
	for _, tokenId := range necessaryFundsKeys {
		neededAmount := necessaryFunds[tokenId]

		partialBalance := new(big.Int).Set(bigZero)
		var selfRecipientKeyNeedsToBeRemoved bool
		var selfRecipientKey string

		balanceIterator, err := ctx.GetStub().GetStateByPartialCompositeKey(balancePrefix, []string{sender, tokenId})
		if err != nil {
			return fmt.Errorf("failed to get state for prefix %v: %v", balancePrefix, err)
		}
		defer balanceIterator.Close()

		// Iterate over keys that store balances and add them to partialBalance until
		// either the necessary amount is reached or the keys ended
		for balanceIterator.HasNext() && partialBalance.Cmp(neededAmount) == -1 {
			queryResponse, err := balanceIterator.Next()
			if err != nil {
				return fmt.Errorf("failed to get the next state for prefix %v: %v", balancePrefix, err)
			}

			partBalAmount, _ := new(big.Int).SetString(string(queryResponse.Value), 10)
			partialBalance = partialBalance.Add(partialBalance, partBalAmount)

			_, compositeKeyParts, err := ctx.GetStub().SplitCompositeKey(queryResponse.Key)
			if err != nil {
				return err
			}

			if compositeKeyParts[2] == sender {
				selfRecipientKeyNeedsToBeRemoved = true
				selfRecipientKey = queryResponse.Key
			} else {
				err = ctx.GetStub().DelState(queryResponse.Key)
				if err != nil {
					return fmt.Errorf("failed to delete the state of %v: %v", queryResponse.Key, err)
				}
			}
		}

		if partialBalance.Cmp(neededAmount) == -1 {
			return fmt.Errorf("sender has insufficient funds for token %v, needed funds: %v, available fund: %v", tokenId, neededAmount, partialBalance)
		} else if partialBalance.Cmp(neededAmount) == 1 {
			// Send the remainder back to the sender
			remainder := new(big.Int).Sub(partialBalance, neededAmount)
			if selfRecipientKeyNeedsToBeRemoved {
				// Set balance for the key that has the same address for sender and recipient
				err = setBalance(ctx, sender, sender, tokenId, remainder)
				if err != nil {
					return err
				}
			} else {
				err = addBalance(ctx, sender, sender, tokenId, remainder)
				if err != nil {
					return err
				}
			}

		} else {
			// Delete self recipient key
			err = ctx.GetStub().DelState(selfRecipientKey)
			if err != nil {
				return fmt.Errorf("failed to delete the state of %v: %v", selfRecipientKey, err)
			}
		}
	}

	return nil
}

// balanceOfHelper returns the balance of the given account
func balanceOfHelper(ctx contractapi.TransactionContextInterface, account string, symbol string) (string, error) {

	if account == "0x" {
		return bigZero.String(), fmt.Errorf("balance query for the zero address")
	}

	balance := new(big.Int).Set(bigZero)

	balanceIterator, err := ctx.GetStub().GetStateByPartialCompositeKey(balancePrefix, []string{account, symbol})
	if err != nil {
		return bigZero.String(), fmt.Errorf("failed to get state for prefix %v: %v", balancePrefix, err)
	}
	defer balanceIterator.Close()

	for balanceIterator.HasNext() {
		queryResponse, err := balanceIterator.Next()
		if err != nil {
			return bigZero.String(), fmt.Errorf("failed to get the next state for prefix %v: %v", balancePrefix, err)
		}

		balAmount, _ := new(big.Int).SetString(string(queryResponse.Value), 10)
		logger.Info(balAmount)
		balance = balance.Add(balance, balAmount)
	}

	return balance.String(), nil
}

// Returns the sorted slice ([]string) copied from the keys of map[uint64]*big.Int
func sortedKeys(m map[string]*big.Int) []string {
	// Copy map keys to slice
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	// Sort the slice
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// txFeeHelper burns fee from the user and reduce total supply accordingly
func txFeeHelper(ctx contractapi.TransactionContextInterface, address string, token string, txFee string, txType string) error {
	minusOne, _ := new(big.Int).SetString("-1", 10)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	err := addTotalSupplyUTXO(ctx, token, new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee))
	if err != nil {
		return err
	}

	// err = addUTXO(ctx, address, bigTxFee, token)
	// if err != nil {
	// 	return err
	// }
	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  bigTxFee.Mul(bigTxFee, minusOne).String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err = ctx.GetStub().PutState(fmt.Sprintf("burnTxFee~%s~%s~%s~%s", ctx.GetStub().GetTxID(), txType, address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}

// check if string is in slice
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func generateTokenAddress(symbol string) string {
	// symbol is case insensitive
	symbol = strings.ToUpper(symbol)
	return "B-" + tokenAddressPrefix + base64Encode(fmt.Sprintf("token-meta-%s", symbol))
}

func base64Encode(str string) string {
	return base64.StdEncoding.EncodeToString([]byte(str))
}

// func base64Decode(str string) (string, bool) {
// 	data, err := base64.StdEncoding.DecodeString(str)
// 	if err != nil {
// 		return "", true
// 	}
// 	return string(data), false
// }

func isDuplicate(slice []string) bool {
	occurred := make(map[string]bool, len(slice))

	for _, item := range slice {
		if occurred[item] {
			return true
		}
		occurred[item] = true
	}
	return false
}
