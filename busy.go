package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
	"github.com/hyperledger/fabric/common/flogging"
)

// Busy chaincode
type Busy struct {
	contractapi.Contract
}

var logger = flogging.MustGetLogger(BUSY_COIN_SYMBOL)

const TRANSFER_FEE string = "1000000000000000"
const PHASE1_STAKING_AMOUNT = "10000000000000000000000"
const BALANCE_EVENT = "BALANCE"
const DEFAULT_CREDS = "defaultCreds"

// Init Initialise chaincocode while deployment
func (bt *Busy) Init(ctx contractapi.TransactionContextInterface) Response {
	response := Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	mspid, _ := ctx.GetClientIdentity().GetMSPID()
	if mspid != "BusyMSP" {
		response.Message = "You are not allowed to issue BUSY coins"
		logger.Error(response.Message)
		return response
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to issue BUSY coins"
		logger.Error(response.Message)
		return response
	}
	// setting Message Config
	config := MessageConfig{
		BigBusyCoins:    "1000000000000000000",
		BusyCoin:        1,
		MessageInterval: 1 * time.Second,
	}
	configAsBytes, _ := json.Marshal(config)
	err := ctx.GetStub().PutState("MessageConfig", configAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	tokenIssueFeeConfig := TokenIssueFee{
		BUSY20: "5000000000000000000000",
		NFT:    "5000000000000000000000",
		GAME:   "5000000000000000000000",
	}
	configAsBytes, _ = json.Marshal(tokenIssueFeeConfig)
	err = ctx.GetStub().PutState("TokenIssueFees", configAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating  token issue fees state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	// setting Voting Config
	votingConfig := VotingConfig{
		MinimumCoins:    "3400000000000000000000",
		PoolFee:         "1700000000000000000000",
		VotingPeriod:    25 * 60 * time.Minute,
		VotingStartTime: 5 * 60 * time.Minute,
	}
	votingConfigAsBytes, _ := json.Marshal(votingConfig)
	err = ctx.GetStub().PutState("VotingConfig", votingConfigAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}
	now, _ := ctx.GetStub().GetTxTimestamp()

	supply, _ := new(big.Int).SetString("255000000000000000000000000", 10)
	token := Token{
		DocType:     "token",
		ID:          0,
		TokenName:   "Busy",
		TokenSymbol: BUSY_COIN_SYMBOL,
		Admin:       response.TxID,
		TotalSupply: supply.String(),
		Decimals:    18,
	}
	tokenAsBytes, _ := json.Marshal(token)
	err = ctx.GetStub().PutState(generateTokenStateAddress(BUSY_COIN_SYMBOL), tokenAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating coin on blockchain : %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, supply)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating coin on blockchain : %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	wallet := Wallet{
		DocType:   "wallet",
		UserID:    commonName,
		Address:   response.TxID,
		Balance:   supply.String(),
		CreatedAt: uint64(now.Seconds),
	}
	walletAsBytes, _ := json.Marshal(wallet)
	err = ctx.GetStub().PutState(response.TxID, walletAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}
	_ = ctx.GetStub().PutState("latestTokenId", []byte(strconv.Itoa(0)))
	user := User{
		DocType:       "user",
		UserID:        commonName,
		DefaultWallet: wallet.Address,
	}
	userAsBytes, _ := json.Marshal(user)
	err = ctx.GetStub().PutState(commonName, userAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	utxo := UTXO{
		DocType: "utxo",
		Address: wallet.Address,
		Amount:  supply.String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s", response.TxID, wallet.Address, token.TokenSymbol), utxoAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	err = ctx.GetStub().PutState("transferFees", []byte(TRANSFER_FEE))
	if err != nil {
		response.Message = fmt.Sprintf("Error while configuring transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	currentStakingLimit, _ := new(big.Int).SetString(PHASE1_STAKING_AMOUNT, 10)
	phaseConfig := PhaseConfig{
		CurrentPhase:          1,
		TotalStakingAddr:      bigZero.String(),
		NextStakingAddrTarget: "1000",
		CurrentStakingLimit:   currentStakingLimit.String(),
	}
	phaseConfigAsBytes, _ := json.Marshal(phaseConfig)
	err = ctx.GetStub().PutState("phaseConfig", phaseConfigAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while initialising phase config: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	phaseUpdateTimeline := map[uint64]PhaseUpdateInfo{
		1: PhaseUpdateInfo{
			UpdatedAt:    uint64(now.Seconds),
			StakingLimit: phaseConfig.CurrentStakingLimit,
		},
	}

	phaseUpdateTimelineAsBytes, _ := json.Marshal(phaseUpdateTimeline)
	err = ctx.GetStub().PutState(PHASE_UPDATE_TIMELINE, phaseUpdateTimelineAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while initialising phase timeline: %s", err.Error())
		logger.Error(response.Message)
		return response
	}

	response.Message = fmt.Sprintf("Successfully issued %s", BUSY_COIN_SYMBOL)
	response.Success = true
	response.Data = token
	logger.Info(response.Message)
	return response
}

// CreateUser creates new user on busy blockchain
func (bt *Busy) CreateUser(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	commonName, _ := getCommonName(ctx)
	userAsBytes, err := ctx.GetStub().GetState(commonName)
	if userAsBytes != nil {
		response.Message = "Nickname is already taken"
		logger.Info(response.Message)
		return response, generateError(406, "ACC001", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "ACC002", response.Message)
	}

	now, _ := ctx.GetStub().GetTxTimestamp()

	wallet := Wallet{
		DocType:   "wallet",
		UserID:    commonName,
		Address:   "B-" + response.TxID,
		CreatedAt: uint64(now.Seconds),
	}
	walletAsBytes, _ := json.Marshal(wallet)
	err = ctx.GetStub().PutState("B-"+response.TxID, walletAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "ACC003", response.Message)
	}

	user := User{
		DocType:       "user",
		UserID:        commonName,
		DefaultWallet: wallet.Address,
		MessageCoins: map[string]int{
			"totalCoins": 0,
		},
	}
	userAsBytes, _ = json.Marshal(user)
	err = ctx.GetStub().PutState(commonName, userAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "ACC003", response.Message)
	}

	response.Message = "User has been successfully registered"
	response.Success = true
	response.Data = wallet.Address
	logger.Info(response.Message)
	return response, nil
}

// CreateStakingAddress create new staking address for user
func (bt *Busy) CreateStakingAddress(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	currentPhaseConfig, err := getPhaseConfig(ctx)
	fmt.Println(currentPhaseConfig)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting phase config: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK001", response.Message)
	}
	now, _ := ctx.GetStub().GetTxTimestamp()

	fmt.Println(currentPhaseConfig.CurrentStakingLimit)
	stakingAmount, _ := new(big.Int).SetString(currentPhaseConfig.CurrentStakingLimit, 10)
	commonName, _ := getCommonName(ctx)
	defaultWalletAddress, _ := getDefaultWalletAddress(ctx, commonName)

	balance, _ := getBalanceHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if balance.Cmp(stakingAmount) == -1 {
		response.Message = "You do not have enough coins to create a staking address"
		logger.Error(response.Message)
		return response, generateError(406, "STK002", response.Message)
	}

	stakingAddress := Wallet{
		DocType:   "stakingAddr",
		UserID:    commonName,
		Address:   "staking-" + response.TxID,
		Balance:   stakingAmount.String(),
		CreatedAt: uint64(now.Seconds),
	}
	txFee, err := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting tx fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK003", response.Message)
	}
	err = transferHelper(ctx, defaultWalletAddress, stakingAddress.Address, stakingAmount, BUSY_COIN_SYMBOL, new(big.Int).Set(bigTxFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while transferring coins to the staking address: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK004", response.Message)
	}
	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK005", response.Message)
	}
	stakingAddrAsBytes, _ := json.Marshal(stakingAddress)
	err = ctx.GetStub().PutState("staking-"+response.TxID, stakingAddrAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK006", response.Message)
	}

	stakingInfo := StakingInfo{
		DocType:              "stakingInfo",
		StakingAddress:       stakingAddress.Address,
		InitialStakingLimit:  stakingAddress.Balance,
		StakedCoins:          stakingAddress.Balance,
		TimeStamp:            uint64(now.Seconds),
		Phase:                currentPhaseConfig.CurrentPhase,
		TotalReward:          bigZero.String(),
		Claimed:              bigZero.String(),
		DefaultWalletAddress: defaultWalletAddress,
		Unstaked:             false,
	}
	stakingInfoAsBytes, _ := json.Marshal(stakingInfo)
	err = ctx.GetStub().PutState(fmt.Sprintf("info~%s", stakingAddress.Address), stakingInfoAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating staking information in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK007", response.Message)
	}

	_, err = updatePhase(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating phase: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "STK008", response.Message)
	}
	// Sending Balance Event
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
	response.Success = true
	response.Message = "Staking address has been successfully created"
	response.Data = stakingInfo
	logger.Info(response.Message)
	return response, nil
}

// GetBalance of specified wallet address
func (bt *Busy) GetBalance(ctx contractapi.TransactionContextInterface, address string, token string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if token == "" {
		token = BUSY_COIN_SYMBOL
	}
	exists, err := ifTokenExists(ctx, token)
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	if !exists || (strings.ToUpper(token) == BUSY_COIN_SYMBOL && token != BUSY_COIN_SYMBOL) {
		response.Message = fmt.Sprintf("Symbol %s does not exist", token)
		logger.Error(response.Message)
		return response, generateError(404, "BAL002", response.Message)
	}

	balance, err := getBalanceHelper(ctx, address, token)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching balance: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL003", response.Message)
	}

	response.Message = "Balance has been successfully fetched"
	response.Success = true
	response.Data = balance.String()
	logger.Info(response.Message)
	return response, nil
}

// GetUser all the wallet and staking address of user with it's balance
func (bt *Busy) GetUser(ctx contractapi.TransactionContextInterface, userID string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	userAsBytes, err := ctx.GetStub().GetState(userID)
	if userAsBytes == nil {
		response.Message = "User does not exist"
		logger.Info(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}

	userDetails := User{}
	if err := json.Unmarshal(userAsBytes, &userDetails); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the sender details %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}

	fmt.Println(userDetails)

	var queryString string = fmt.Sprintf(`{
		"selector": {
			"userId": "%s",
			"docType": "stakingAddr"
		 } 
	}`, userID)
	resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user wallets: %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	defer resultIterator.Close()

	var wallet Wallet
	responseData := map[string]interface{}{}
	for resultIterator.HasNext() {
		data, _ := resultIterator.Next()
		_ = json.Unmarshal(data.Value, &wallet)
		balance, _ := getBalanceHelper(ctx, wallet.Address, BUSY_COIN_SYMBOL)
		walletDetails := make(map[string]interface{}, 3)
		walletDetails["balance"] = balance.String()
		walletDetails["token"] = BUSY_COIN_SYMBOL
		walletDetails["createdAt"] = wallet.CreatedAt
		responseData[wallet.Address] = walletDetails
	}

	defaultWalletDetails := make(map[string]interface{}, 3)

	walletAsBytes, _ := ctx.GetStub().GetState(userDetails.DefaultWallet)
	if err := json.Unmarshal(walletAsBytes, &wallet); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the sender details %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}

	balance, _ := getBalanceHelper(ctx, userDetails.DefaultWallet, BUSY_COIN_SYMBOL)
	defaultWalletDetails["balance"] = balance.String()
	defaultWalletDetails["token"] = BUSY_COIN_SYMBOL
	defaultWalletDetails["createdAt"] = wallet.CreatedAt

	responseData[userDetails.DefaultWallet] = defaultWalletDetails

	responseData["messageCoins"] = userDetails.MessageCoins
	response.Message = "Balance has been successfully fetched"
	response.Success = true
	response.Data = responseData
	logger.Info(response.Message)
	return response, nil
}

func (bt *Busy) GetTokenIssueFee(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	feesAsBytes, err := ctx.GetStub().GetState("TokenIssueFees")
	if feesAsBytes == nil {
		response.Message = "TokenIssueFees does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "TOKF001", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching TokenIssueFees from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOKF002", response.Message)
	}

	tokenIssueFees := TokenIssueFee{}
	if err := json.Unmarshal(feesAsBytes, &tokenIssueFees); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving TokenIssueFees %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOKF003", response.Message)
	}

	response.Data = tokenIssueFees
	response.Message = "Current token issue fee fee has been successfully fetched"
	response.Success = true
	return response, nil
}

func getTokenIssueFeeForTokenType(ctx contractapi.TransactionContextInterface, tokenType string) (string, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	feesAsBytes, err := ctx.GetStub().GetState("TokenIssueFees")
	if feesAsBytes == nil {
		response.Message = "Token Issue Fees does not exist"
		logger.Info(response.Message)
		return "", fmt.Errorf(response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return "", fmt.Errorf(response.Message)
	}

	tokenIssueFees := TokenIssueFee{}
	if err := json.Unmarshal(feesAsBytes, &tokenIssueFees); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the sender details %s", err.Error())
		logger.Error(response.Message)
		return "", fmt.Errorf(response.Message)
	}

	if strings.ToUpper(tokenType) == "BUSY20" {
		return tokenIssueFees.BUSY20, nil
	} else if strings.ToUpper(tokenType) == "NFT" {
		return tokenIssueFees.NFT, nil
	} else if strings.ToUpper(tokenType) == "GAME" {
		return tokenIssueFees.GAME, nil
	} else {
		return "", fmt.Errorf("invalid token type , please select token type from [BUSY20, NFT, GAME]")
	}
}

func (bt *Busy) SetTokenIssueFee(ctx contractapi.TransactionContextInterface, tokenType string, newFee string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	feesAsBytes, err := ctx.GetStub().GetState("TokenIssueFees")
	if feesAsBytes == nil {
		response.Message = "TokenIssueFees does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "UTKF001", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching TokenIssueFees from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTKF002", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to update Token Issue Fee"
		logger.Error(response.Message)
		return response, generateError(403, "UTKF003", response.Message)
	}

	tokenIssueFees := TokenIssueFee{}
	if err := json.Unmarshal(feesAsBytes, &tokenIssueFees); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the sender details %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTKF004", response.Message)
	}
	if strings.ToUpper(tokenType) == "BUSY20" {
		tokenIssueFees.BUSY20 = newFee
	} else if strings.ToUpper(tokenType) == "NFT" {
		tokenIssueFees.NFT = newFee
	} else if strings.ToUpper(tokenType) == "GAME" {
		tokenIssueFees.GAME = newFee
	} else {
		return response, generateError(412, "UTKF005", "invalid token type , please select token type from [BUSY20, NFT, GAME]")
	}

	feesAsBytes, _ = json.Marshal(tokenIssueFees)
	err = ctx.GetStub().PutState("TokenIssueFees", feesAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating token issue fees on blockchain : %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTKF006", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses:  []UserAddress{},
		TransactionFee: bigZero.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "Token Issue Fee is set successfully"
	response.Success = true
	response.Data = tokenIssueFees
	logger.Info(response.Message)
	return response, nil
}

// IssueToken issue token in default wallet address of invoker
func (bt *Busy) IssueToken(ctx contractapi.TransactionContextInterface, tokenName string, symbol string, amount string, decimals uint64, metadata TokenMetaData) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	bigAmount, isConverted := new(big.Int).SetString(amount, 10)
	if !isConverted {
		response.Message = "Error encountered converting amount"
		logger.Error(response.Message)
		return response, generateError(412, "TOK003", response.Message)
	}
	if bigAmount.Cmp(bigZero) == 0 {
		response.Message = "Amount can not be zero"
		logger.Error(response.Message)
		return response, generateError(412, "TOK004", response.Message)
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	if decimals > 18 {
		response.Message = "Decimals have to be in range of 1-18"
		logger.Error(response.Message)
		return response, generateError(412, "TOK005", response.Message)
	}

	if metadata.Logo == "" || metadata.Type == "" {
		response.Message = "Invalid Metadata"
		logger.Error(response.Message)
		return response, generateError(423, "TOK006", response.Message)
	}

	if strings.ToUpper(symbol) == BUSY_COIN_SYMBOL || strings.ToUpper(tokenName) == BUSY_COIN_SYMBOL {
		response.Message = "Symbol/TokenName cannot be BUSY!"
		logger.Error(response.Message)
		return response, generateError(412, "TOK007", response.Message)
	}
	exp := regexp.MustCompile(`^[\w]+([-\s]{1}[\w]+)*$`)
	tokenLength := regexp.MustCompile(`^.{3,20}$`)
	// checking for token Name
	if !exp.MatchString(tokenName) || !tokenLength.MatchString(tokenName) {
		response.Message = "Invalid token name"
		logger.Error(response.Message)
		return response, generateError(412, "TOK008", response.Message)
	}

	symbolLength := regexp.MustCompile(`^.{3,5}$`)
	// checking for tokenSymbol
	if !exp.MatchString(symbol) || !symbolLength.MatchString(symbol) {
		response.Message = "Invalid token symbol"
		logger.Error(response.Message)
		return response, generateError(412, "TOK009", response.Message)
	}

	if metadata.Type != "BUSY20" {
		response.Message = "Only BUSY20 is supported"
		logger.Error(response.Message)
		return response, generateError(412, "TOK010", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	tokenFeeString, _ := getTokenIssueFeeForTokenType(ctx, "BUSY20")
	issueTokenFee, _ := new(big.Int).SetString(tokenFeeString, 10)
	minusOne, _ := new(big.Int).SetString("-1", 10)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching user's default wallet: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK011", response.Message)
	}
	balance, err := getBalanceHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching user's balance: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK012", response.Message)
	}
	if balance.Cmp(issueTokenFee) == -1 {
		response.Message = "You do not have enough coins to issue token!"
		logger.Error(response.Message)
		return response, generateError(402, "TOK013", response.Message)
	}

	tokenAddress := generateTokenAddress(symbol)
	// checking if the token already exists
	busyTokensInfoAsBytes, err := ctx.GetStub().GetState(tokenAddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK014", response.Message)
	}
	if busyTokensInfoAsBytes != nil {
		response.Message = "NFT/Game token with same name already exists"
		logger.Info(response.Message)
		return response, generateError(409, "TOK015", response.Message)
	}

	var token Token
	tokenAsBytes, err := ctx.GetStub().GetState(generateTokenStateAddress(symbol))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK016", response.Message)
	}

	tokenIdAsBytes, err := ctx.GetStub().GetState("latestTokenId")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching latest token id: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK017", response.Message)
	}
	latestTokenID, _ := strconv.Atoi(string(tokenIdAsBytes))

	if tokenAsBytes == nil {
		var queryString string = fmt.Sprintf(`{
			"selector": {
				"docType": "token",
				"tokenName": "%s"
			 } 
		}`, tokenName)
		resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while fetching query data: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "TOK018", response.Message)
		}
		defer resultIterator.Close()
		if resultIterator.HasNext() {
			response.Message = fmt.Sprintf("Token %s already exists", tokenName)
			logger.Error(response.Message)
			return response, generateError(409, "TOK019", response.Message)
		}

		_ = ctx.GetStub().PutState("latestTokenId", []byte(strconv.Itoa(latestTokenID+1)))
		token := Token{
			DocType:      "token",
			ID:           uint64(latestTokenID + 1),
			TokenName:    tokenName,
			TokenSymbol:  symbol,
			Admin:        defaultWalletAddress,
			TotalSupply:  bigAmount.String(),
			Decimals:     decimals,
			MetaData:     metadata,
			TokenAddress: generateTokenStateAddress(symbol),
		}
		tokenAsBytes, _ = json.Marshal(token)
		err = ctx.GetStub().PutState(generateTokenStateAddress(symbol), tokenAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating token on blockchain : %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "TOK020", response.Message)
		}

		err = addTotalSupplyUTXO(ctx, symbol, bigAmount)
		if err != nil {
			response.Message = fmt.Sprintf("Error while creating total supply UTXO : %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "TOK021", response.Message)
		}
		response.Data = token
	} else {

		response.Message = fmt.Sprintf("Token with symbol %s already exists", token.TokenSymbol)
		logger.Error(response.Message)
		return response, generateError(409, "TOK022", response.Message)
	}

	issuerAddress, _ := getDefaultWalletAddress(ctx, commonName)
	err = addUTXO(ctx, issuerAddress, bigAmount, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while generating UTXO for new token: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK023", response.Message)
	}

	err = addUTXO(ctx, defaultWalletAddress, new(big.Int).Set(issueTokenFee).Mul(issueTokenFee, minusOne), BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while burning fee for issue token: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TOK024", response.Message)
	}
	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, new(big.Int).Set(issueTokenFee).Mul(minusOne, issueTokenFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while burning issue token fee from total supply: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(400, "TOK025", response.Message)
	}

	// Sending Balance Event
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultWalletAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
			{
				Address: defaultWalletAddress,
				Token:   symbol,
			},
		},
		TransactionFee: issueTokenFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	response.Message = fmt.Sprintf("Token %s has been successfully issued", symbol)
	response.Success = true
	logger.Info(response.Message)
	return response, nil
}

// Transfer transfer given amount from invoker's identity to specified identity
func (bt *Busy) Transfer(ctx contractapi.TransactionContextInterface, recipiant string, amount string, token string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if amount == "0" {
		response.Message = "Zero amount can not be transferred"
		logger.Error(response.Message)
		return response, generateError(412, "TRA001", response.Message)
	}

	commonName, _ := getCommonName(ctx)
	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if commonName != "busy_network" && err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	// check if token exists
	exists, err := ifTokenExists(ctx, token)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching the details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TRA002", response.Message)
	}
	if !exists || (strings.ToUpper(token) == BUSY_COIN_SYMBOL && token != BUSY_COIN_SYMBOL) {
		response.Message = fmt.Sprintf("Symbol %s does not exist", token)
		logger.Error(response.Message)
		return response, generateError(404, "TRA003", response.Message)
	}

	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(recipiant)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TRA004", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", recipiant)
		logger.Error(response.Message)
		return response, generateError(404, "TRA005", response.Message)
	}

	// Fetch current transfer fee
	transferFeesAsBytes, err := ctx.GetStub().GetState("transferFees")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching transfer fee %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TRA006", response.Message)
	}
	bigTransferFee, _ := new(big.Int).SetString(string(transferFeesAsBytes), 10)

	bigAmount, _ := new(big.Int).SetString(amount, 10)

	if bigAmount == nil {
		response.Message = "Amount is invalid"
		logger.Error(response.Message)
		return response, generateError(412, "TRA007", response.Message)
	}

	if bigAmount.Cmp(bigZero) == -1 {
		response.Message = "Transfer amount is invalid"
		logger.Error(response.Message)
		return response, generateError(412, "TRA008", response.Message)
	}

	if token == "" {
		token = BUSY_COIN_SYMBOL
	}
	sender, _ := getCommonName(ctx)
	userAsBytes, err := ctx.GetStub().GetState(sender)
	if userAsBytes == nil {
		response.Message = "User does not exist"
		logger.Error(response.Message)
		return response, generateError(404, "TRA009", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching user %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(400, "TRA010", response.Message)
	}
	var user User
	_ = json.Unmarshal(userAsBytes, &user)

	if user.DefaultWallet == recipiant {
		response.Message = "It is not possible to transfer to your address"
		logger.Error(response.Message)
		return response, generateError(409, "TRA011", response.Message)
	}
	isStakingAddress := strings.HasPrefix(recipiant, "staking-")
	if isStakingAddress {
		var wallet Wallet
		var queryString string = fmt.Sprintf(`{
			"selector": {
				"docType": "stakingAddr",
				"address": "%s"
			 } 
		}`, recipiant)
		resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while fetching user wallets: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "TRA012", response.Message)
		}
		defer resultIterator.Close()

		if resultIterator.HasNext() {
			data, _ := resultIterator.Next()
			_ = json.Unmarshal(data.Value, &wallet)
			if wallet.UserID != sender {
				response.Message = "It is not possible to make a transfer to the staking addresses"
				logger.Error(response.Message)
				return response, generateError(406, "TRA013", response.Message)
			}
		} else {
			response.Message = "Staking address does not exist"
			logger.Error(response.Message)
			return response, generateError(404, "TRA014", response.Message)
		}
	}
	if sender == "busy_network" {
		bigTransferFee = bigZero
	}

	err = transferHelper(ctx, user.DefaultWallet, recipiant, bigAmount, token, bigTransferFee)
	if err != nil {
		response.Message = "You do not have enough amount to transfer"
		logger.Error(response.Message)
		return response, generateError(400, "TRA014", response.Message)
	}

	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, bigTransferFee.Mul(minusOne, bigTransferFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(400, "TRA015", response.Message)
	}

	userAddresses := []UserAddress{
		{
			Address: user.DefaultWallet,
			Token:   BUSY_COIN_SYMBOL,
		},
		{
			Address: recipiant,
			Token:   token,
		},
	}
	if token != BUSY_COIN_SYMBOL {
		userAddresses = append(userAddresses, UserAddress{
			Address: user.DefaultWallet,
			Token:   token,
		})
	}
	// Sending Balance Event
	balanceData := BalanceEvent{
		UserAddresses:  userAddresses,
		TransactionFee: bigTransferFee.Mul(bigTransferFee, minusOne).String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "Transfer has been successfully accepted"
	logger.Info(response.Message)
	response.Success = true
	return response, nil
}

// GetTotalSupply get total supply of specified token
func (bt *Busy) GetTotalSupply(ctx contractapi.TransactionContextInterface, symbol string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if symbol == "" {
		symbol = BUSY_COIN_SYMBOL
	}
	var token Token
	tokenAsBytes, err := ctx.GetStub().GetState(generateTokenStateAddress(symbol))
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TSUP004", response.Message)
	}
	if tokenAsBytes == nil {
		response.Message = fmt.Sprintf("Symbol %s does not exist", symbol)
		logger.Error(response.Message)
		return response, generateError(404, "TSUP005", response.Message)
	}
	_ = json.Unmarshal(tokenAsBytes, &token)

	totalSupply, _, err := pruneUTXOs(ctx, TOTAL_SUPPLY_KEY, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error while pruning utxo: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "TSUP006", response.Message)
	}
	token.TotalSupply = totalSupply.String()

	response.Message = "Total supply has been successfully fetched"
	logger.Info(response.Message)
	response.Data = fmt.Sprintf("%s %s", token.TotalSupply, symbol)
	response.Success = true
	return response, nil
}

// Burn reduct balance from user wallet and reduce total supply
func (bt *Busy) Burn(ctx contractapi.TransactionContextInterface, address string, amount string, symbol string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if amount == "0" {
		response.Message = "It is not possible to burn zero amount"
		logger.Error(response.Message)
		return response, generateError(400, "BURN001", response.Message)
	}
	exists, err := ifTokenExists(ctx, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BURN002", response.Message)
	}
	if !exists {
		response.Message = fmt.Sprintf("Symbol %s does not exist", symbol)
		logger.Error(response.Message)
		return response, generateError(404, "BURN003", response.Message)
	}

	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error %s fetching common name", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BURN004", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if commonName != "busy_network" && err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error %s fetching default wallet", err.Error())
		logger.Error(response.Message)
		return response, generateError(400, "BURN005", response.Message)
	}
	tokenAsBytes, _ := ctx.GetStub().GetState(generateTokenStateAddress(symbol))
	token := Token{}
	_ = json.Unmarshal(tokenAsBytes, &token)

	if token.Admin != address {
		response.Message = fmt.Sprintf("%s not allowed to burn %s", address, symbol)
		logger.Error(response.Message)
		return response, generateError(403, "BURN006", response.Message)
	}

	if defaultWalletAddress != address {
		response.Message = fmt.Sprintf("Wallet address %s is not allowed to burn", address)
		logger.Error(response.Message)
		return response, generateError(403, "BURN007", response.Message)
	}

	balance, err := getBalanceHelper(ctx, address, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching balance: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BURN008", response.Message)
	}
	bigAmount, _ := new(big.Int).SetString(amount, 10)
	if balance.Cmp(bigAmount) == -1 {
		response.Message = "There is not enough balance in the wallet"
		logger.Error(response.Message)
		return response, generateError(402, "BURN009", response.Message)
	}

	negetiveBigAmount, _ := new(big.Int).SetString("-"+amount, 10)

	err = addUTXO(ctx, address, negetiveBigAmount, symbol)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while burning: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BURN010", response.Message)
	}

	err = burnTxFee(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning tx fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BURN011", response.Message)
	}

	txFee, _ := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	bigTxFee = new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee)
	if symbol == BUSY_COIN_SYMBOL {
		negetiveBigAmount = negetiveBigAmount.Add(negetiveBigAmount, bigTxFee)
		err = addTotalSupplyUTXO(ctx, symbol, negetiveBigAmount)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BURN012", response.Message)
		}
	} else {
		err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, bigTxFee)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BURN012", response.Message)
		}

		err = addTotalSupplyUTXO(ctx, symbol, negetiveBigAmount)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BURN012", response.Message)
		}
	}

	userAddresses := []UserAddress{
		{
			Address: defaultWalletAddress,
			Token:   symbol,
		},
	}

	if symbol != BUSY_COIN_SYMBOL {
		userAddresses = append(userAddresses, UserAddress{
			Address: address,
			Token:   BUSY_COIN_SYMBOL,
		})
	}
	balanceData := BalanceEvent{
		UserAddresses:  userAddresses,
		TransactionFee: txFee,
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "Burn has been successful"
	logger.Info(response.Message)
	response.Success = true
	return response, nil
}

// MultibeneficiaryVestingV1 vesting v1
func (bt *Busy) MultibeneficiaryVestingV1(ctx contractapi.TransactionContextInterface, recipient string, amount string, numerator uint64, denominator uint64, releaseAt uint64) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if amount == "0" {
		response.Message = "Zero amount can not be vested"
		logger.Error(response.Message)
		return response, generateError(412, "VONE001", response.Message)
	}

	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(recipient)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VONE002", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", recipient)
		logger.Error(response.Message)
		return response, generateError(404, "VONE003", response.Message)
	}

	now, _ := ctx.GetStub().GetTxTimestamp()
	mspid, _ := ctx.GetClientIdentity().GetMSPID()
	if mspid != "BusyMSP" {
		response.Message = "You are not allowed to create vesting"
		logger.Error(response.Message)
		return response, generateError(403, "VONE004", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to create vesting"
		logger.Error(response.Message)
		return response, generateError(403, "VONE004", response.Message)
	}
	bigAmount, _ := new(big.Int).SetString(amount, 10)
	if bigAmount.Cmp(bigZero) == 0 {
		response.Message = "Zero amount can not be vested"
		logger.Error(response.Message)
		return response, generateError(400, "VONE001", response.Message)
	}
	adminAddress, _ := getDefaultWalletAddress(ctx, commonName)
	balance, _ := getBalanceHelper(ctx, adminAddress, BUSY_COIN_SYMBOL)
	if balance.Cmp(bigAmount) == -1 {
		response.Message = "There is not enough balance in the wallet"
		logger.Error(response.Message)
		return response, generateError(402, "VONE005", response.Message)
	}

	lockedTokenAsBytes, _ := ctx.GetStub().GetState(fmt.Sprintf("vesting~%s", recipient))
	if lockedTokenAsBytes != nil {
		response.Message = fmt.Sprintf("Vesting for wallet %s already exists", recipient)
		logger.Error(response.Message)
		return response, generateError(409, "VONE006", response.Message)
	}
	if releaseAt < uint64(now.Seconds) {
		response.Message = "Release time of vesting has to be in the future"
		logger.Error(response.Message)
		return response, generateError(412, "VONE007", response.Message)
	}

	totalAmount := new(big.Int).Set(bigAmount)
	currentVesting, err := calculatePercentage(bigAmount, numerator, denominator)
	if err != nil {
		response.Message = fmt.Sprintf("Error while calculating vesting percentage: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VONE008", response.Message)
	}
	txFee, err := getCurrentTxFee(ctx)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting tx fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VONE009", response.Message)
	}
	err = transferHelper(ctx, adminAddress, recipient, currentVesting, BUSY_COIN_SYMBOL, new(big.Int).Set(bigTxFee))
	if err != nil {
		response.Message = "You do not have enough amount to transfer"
		logger.Error(response.Message)
		return response, generateError(402, "VONE010", response.Message)
	}
	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VONE011", response.Message)
	}

	lockedToken := LockedTokens{
		DocType:        "lockedToken",
		TotalAmount:    totalAmount.String(),
		ReleasedAmount: currentVesting.String(),
		StartedAt:      uint64(now.Seconds),
		ReleaseAt:      releaseAt,
	}
	lockedTokenAsBytes, _ = json.Marshal(lockedToken)
	err = ctx.GetStub().PutState(fmt.Sprintf("vesting~%s", recipient), lockedTokenAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while adding vesting schedule: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VONE012", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: recipient,
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
	response.Message = "Vesting has been scheduled successfully"
	logger.Info(response.Message)
	response.Success = true
	return response, nil
}

// MultibeneficiaryVestingV2 vesting v2
func (bt *Busy) MultibeneficiaryVestingV2(ctx contractapi.TransactionContextInterface, recipient string, amount string, startAt uint64, releaseAt uint64) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	if amount == "0" {
		response.Message = "Zero amount can not be vested"
		logger.Error(response.Message)
		return response, generateError(412, "VTWO001", response.Message)
	}

	// check if wallet already exists
	walletAsBytes, err := ctx.GetStub().GetState(recipient)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VTWO002", response.Message)
	}
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", recipient)
		logger.Error(response.Message)
		return response, generateError(404, "VTWO003", response.Message)
	}

	now, _ := ctx.GetStub().GetTxTimestamp()
	mspid, _ := ctx.GetClientIdentity().GetMSPID()
	if mspid != "BusyMSP" {
		response.Message = "You are not allowed to create vesting"
		logger.Error(response.Message)
		return response, generateError(403, "VTWO004", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to create vesting"
		logger.Error(response.Message)
		return response, generateError(404, "VTWO004", response.Message)
	}
	bigAmount, _ := new(big.Int).SetString(amount, 10)
	if bigAmount.Cmp(bigZero) == 0 {
		response.Message = "Zero amount can not be vested"
		logger.Error(response.Message)
		return response, generateError(412, "VTWO001", response.Message)
	}
	adminAddress, _ := getDefaultWalletAddress(ctx, commonName)
	balance, _ := getBalanceHelper(ctx, adminAddress, BUSY_COIN_SYMBOL)
	if balance.Cmp(bigAmount) == -1 {
		response.Message = "There is not enough balance in the wallet"
		logger.Error(response.Message)
		return response, generateError(402, "VTWO005", response.Message)
	}
	if releaseAt < startAt {
		response.Message = "Release time of vesting has to be greater then start time"
		logger.Error(response.Message)
		return response, generateError(412, "VTWO006", response.Message)
	}

	lockedTokenAsBytes, _ := ctx.GetStub().GetState(fmt.Sprintf("vesting~%s", recipient))
	if lockedTokenAsBytes != nil {
		response.Message = fmt.Sprintf("Vesting for wallet %s already exists", recipient)
		logger.Error(response.Message)
		return response, generateError(409, "VTWO007", response.Message)
	}
	if releaseAt < uint64(now.Seconds) {
		response.Message = "Release time of vesting has to be in the future"
		logger.Error(response.Message)
		return response, generateError(412, "VTWO008", response.Message)
	}

	if startAt < uint64(now.Seconds) {
		response.Message = "Start time of vesting has to be in the future"
		logger.Error(response.Message)
		return response, generateError(412, "VTWO009", response.Message)
	}

	totalAmount := new(big.Int).Set(bigAmount)
	lockedToken := LockedTokens{
		DocType:        "lockedToken",
		TotalAmount:    totalAmount.String(),
		ReleasedAmount: "0",
		StartedAt:      startAt,
		ReleaseAt:      releaseAt,
	}
	lockedTokenAsBytes, _ = json.Marshal(lockedToken)
	err = ctx.GetStub().PutState(fmt.Sprintf("vesting~%s", recipient), lockedTokenAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while adding vesting schedule: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VTWO010", response.Message)
	}
	sender, _ := getCommonName(ctx)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, sender)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VTWO011", response.Message)
	}
	err = burnTxFeeWithTotalSupply(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VTWO012", response.Message)
	}
	txFee, _ := getCurrentTxFee(ctx)
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: recipient,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: txFee,
		TransactionId:  response.TxID,
	}

	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	response.Message = "Vesting has been scheduled successfully"
	logger.Info(response.Message)
	response.Success = true
	return response, nil
}

// GetLockedTokens get entry of vesting schedule for wallet address
func (bt *Busy) GetLockedTokens(ctx contractapi.TransactionContextInterface, address string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	lockedTokenAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("vesting~%s", address))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while getting vesting details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "GLOK001", response.Message)
	}
	if lockedTokenAsBytes == nil {
		response.Message = fmt.Sprintf("Vesting entry does not exist for wallet %s", address)
		logger.Error(response.Message)
		return response, generateError(404, "GLOK002", response.Message)
	}
	var lockedToken LockedTokens
	_ = json.Unmarshal(lockedTokenAsBytes, &lockedToken)

	response.Message = "Vesting has been successfully fetched"
	logger.Info(response.Message)
	response.Data = lockedToken
	response.Success = true
	return response, nil
}

// AttemptUnlock
func (bt *Busy) AttemptUnlock(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	commonName, _ := getCommonName(ctx)
	now, _ := ctx.GetStub().GetTxTimestamp()
	walletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "AULK001", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	fee, _ := getCurrentTxFee(ctx)
	bigFee, _ := new(big.Int).SetString(fee, 10)

	balance, _ := getBalanceHelper(ctx, walletAddress, BUSY_COIN_SYMBOL)
	if bigFee.Cmp(balance) == 1 {
		response.Message = "There is not enough balance for tx fee in the wallet"
		logger.Error(response.Message)
		return response, generateError(402, "AULK002", response.Message)
	}

	lockedTokenAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("vesting~%s", walletAddress))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while getting vesting details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "AULK003", response.Message)
	}
	if lockedTokenAsBytes == nil {
		response.Message = fmt.Sprintf("Vesting entry does not exist for %s", walletAddress)
		logger.Error(response.Message)
		return response, generateError(404, "AULK004", response.Message)
	}
	var lockedToken LockedTokens
	_ = json.Unmarshal(lockedTokenAsBytes, &lockedToken)
	bigTotalAmount, _ := new(big.Int).SetString(lockedToken.TotalAmount, 10)
	bigReleasedAmount, _ := new(big.Int).SetString(lockedToken.ReleasedAmount, 10)
	bigStartedAt := new(big.Int).SetUint64(lockedToken.StartedAt)
	bigReleasedAt := new(big.Int).SetUint64(lockedToken.ReleaseAt)
	bigNow := new(big.Int).SetUint64(uint64(now.Seconds))

	if lockedToken.StartedAt > uint64(now.Seconds) {
		response.Message = "Vesting has not started yet"
		logger.Info(response.Message)
		return response, generateError(425, "AULK005", response.Message)
	}
	if lockedToken.ReleaseAt <= uint64(now.Seconds) {
		if lockedToken.TotalAmount == lockedToken.ReleasedAmount {
			response.Message = "There are no funds to claim from the vesting"
			response.Success = false
			logger.Error(response.Message)
			return response, generateError(400, "AULK006", response.Message)
		}
		amountToReleaseNow := bigTotalAmount.Sub(bigTotalAmount, bigReleasedAmount)
		lockedToken.ReleasedAmount = lockedToken.TotalAmount
		err = addUTXO(ctx, walletAddress, amountToReleaseNow, BUSY_COIN_SYMBOL)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while claiming: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "AULK007", response.Message)
		}
		lockedTokenAsBytes, _ := json.Marshal(lockedToken)
		err = ctx.GetStub().PutState(fmt.Sprintf("vesting~%s", walletAddress), lockedTokenAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while updating vesting schedule: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "AULK008", response.Message)
		}

		// Burning the tx fee in attempt Unlock.
		err = burnTxFeeWithTotalSupply(ctx, walletAddress, BUSY_COIN_SYMBOL)
		if err != nil {
			response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "AULK009", response.Message)
		}
		txFee, _ := getCurrentTxFee(ctx)
		balanceData := BalanceEvent{
			UserAddresses: []UserAddress{
				{
					Address: walletAddress,
					Token:   BUSY_COIN_SYMBOL,
				},
			},
			TransactionFee: txFee,
			TransactionId:  response.TxID,
		}
		balanceAsBytes, _ := json.Marshal(balanceData)
		err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "BAL001", response.Message)
		}
		response.Message = "All tokens have been already unlocked"
		response.Success = true
		logger.Info(response.Message)
		return response, nil
	}
	releasableAmount := bigTotalAmount.Mul(bigNow.Sub(bigNow, bigStartedAt), bigTotalAmount).Div(bigTotalAmount, bigReleasedAt.Sub(bigReleasedAt, bigStartedAt))
	if releasableAmount.String() == "0" {
		response.Message = "There is nothing to release at this time"
		logger.Error(response.Message)
		return response, generateError(425, "AULK010", response.Message)
	}
	if releasableAmount.Cmp(bigTotalAmount) == 1 {
		response.Message = "There is nothing to release at this time"
		logger.Error(response.Message)
		return response, generateError(425, "AULK010", response.Message)
	}
	releasableAmount = releasableAmount.Sub(releasableAmount, bigReleasedAmount)
	_ = addUTXO(ctx, walletAddress, releasableAmount, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while claiming: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "AULK007", response.Message)
	}
	bigReleasedAmount = bigReleasedAmount.Add(bigReleasedAmount, releasableAmount)
	lockedToken.ReleasedAmount = bigReleasedAmount.String()
	lockedTokenAsBytes, _ = json.Marshal(lockedToken)
	err = ctx.GetStub().PutState(fmt.Sprintf("vesting~%s", walletAddress), lockedTokenAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating vesting schedule: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "AULK008", response.Message)
	}

	err = burnTxFeeWithTotalSupply(ctx, walletAddress, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "AULK009", response.Message)
	}
	txFee, _ := getCurrentTxFee(ctx)
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: walletAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: txFee,
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "All tokens have been already unlocked"
	response.Success = true
	logger.Info(response.Message)
	return response, nil
}

func (bt *Busy) UpdateTransferFee(ctx contractapi.TransactionContextInterface, newTransferFee string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	mspid, _ := ctx.GetClientIdentity().GetMSPID()
	if mspid != "BusyMSP" {
		response.Message = "You are not allowed to set the transaction fee"
		logger.Error(response.Message)
		return response, generateError(403, "UTRF001", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to set the transaction fee"
		logger.Error(response.Message)
		return response, generateError(403, "UTRF001", response.Message)
	}

	err := ctx.GetStub().PutState("transferFees", []byte(newTransferFee))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTRF002", response.Message)
	}

	sender, _ := getCommonName(ctx)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, sender)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTRF003", response.Message)
	}
	err = burnTxFeeWithTotalSupply(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error while burning transfer fee: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UTRF004", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses:  []UserAddress{},
		TransactionFee: bigZero.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "Transfer fee has been successfully updated"
	response.Success = true
	response.Data = newTransferFee
	logger.Error(response.Message)
	return response, nil
}

func (bt *Busy) GetTokenDetails(ctx contractapi.TransactionContextInterface, tokenSymbol string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	tokenAsBytes, err := ctx.GetStub().GetState(generateTokenStateAddress(tokenSymbol))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching token details: %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	if tokenAsBytes == nil {
		response.Message = fmt.Sprintf("Symbol %s does not exist", tokenSymbol)
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	var token Token
	_ = json.Unmarshal(tokenAsBytes, &token)

	response.Message = "Token has been successfully fetched"
	response.Success = true
	response.Data = token
	logger.Info(response.Message)
	return response, nil
}

func (bt *Busy) GetStakingInfo(ctx contractapi.TransactionContextInterface, walletId string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	walletAsBytes, err := ctx.GetStub().GetState(walletId)
	if walletAsBytes == nil {
		response.Message = fmt.Sprintf("Wallet %s does not exist", walletId)
		logger.Info(response.Message)
		return response, generateError(404, "SKI001", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SKI002", response.Message)
	}

	walletDetails := Wallet{}
	if err := json.Unmarshal(walletAsBytes, &walletDetails); err != nil {
		response.Message = fmt.Sprintf("Error occurred while retrieving sender details %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SKI003", response.Message)
	}

	currentPhaseConfig, err := getPhaseConfig(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error while initializing phase config: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SKI004", response.Message)
	}

	var queryString string = fmt.Sprintf(`{
		"selector": {
			"userId": "%s",
			"docType": "stakingAddr"
		 } 
	}`, walletDetails.UserID)
	resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SKI005", response.Message)
	}
	defer resultIterator.Close()

	var stakingAddr Wallet
	responseData := map[string]interface{}{}
	for resultIterator.HasNext() {
		tmpData := map[string]interface{}{}
		data, _ := resultIterator.Next()
		_ = json.Unmarshal(data.Value, &stakingAddr)
		stakingInfo, _ := getStakingInfo(ctx, stakingAddr.Address)
		tmpData["claimed"] = stakingInfo.Claimed
		reward, err := countStakingReward(ctx, stakingInfo.StakingAddress)
		if err != nil {
			response.Message = fmt.Sprintf("Error occurred while counting staking reward: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SKI006", response.Message)
		}
		tmpData["totalReward"] = reward.String()
		tmpData["creationTime"] = stakingInfo.TimeStamp
		tmpData["stakedCoins"] = stakingInfo.StakedCoins
		tmpData["initialStakingLimit"] = stakingInfo.InitialStakingLimit
		tmpData["currentStakingLimit"] = currentPhaseConfig.CurrentStakingLimit
		stakedCoins, _ := new(big.Int).SetString(stakingInfo.StakedCoins, 10)
		currentStakingLimit, _ := new(big.Int).SetString(currentPhaseConfig.CurrentStakingLimit, 10)
		stakedDifference := new(big.Int).Sub(stakedCoins, currentStakingLimit)

		claimed, _ := new(big.Int).SetString(stakingInfo.Claimed, 10)
		totalReward, _ := new(big.Int).SetString(reward.String(), 10)
		availableReward := new(big.Int).Add(totalReward, stakedDifference)
		availableToClaim := new(big.Int).Sub(availableReward, claimed)
		tmpData["availableToClaim"] = availableToClaim.String()
		tmpData["stakedDifference"] = stakedDifference.String()
		responseData[stakingAddr.Address] = tmpData
	}

	response.Message = "Staking details have been successfully fetched"
	response.Success = true
	response.Data = responseData
	logger.Info(response.Message)
	return response, nil
}

func (bt *Busy) Claim(ctx contractapi.TransactionContextInterface, stakingAddr string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	commonName, _ := getCommonName(ctx)
	fee, _ := getCurrentTxFee(ctx)
	bigFee, _ := new(big.Int).SetString(fee, 10)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(404, "CLM001", response.Message)
	}

	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	stakingAddrAsBytes, err := ctx.GetStub().GetState(stakingAddr)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching staking address: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM002", response.Message)
	}
	if stakingAddrAsBytes == nil {
		response.Message = fmt.Sprintf("No Staking address %s found", stakingAddr)
		logger.Error(response.Message)
		return response, generateError(404, "CLM003", response.Message)
	}
	var stAddr Wallet
	_ = json.Unmarshal(stakingAddrAsBytes, &stAddr)
	if stAddr.UserID != commonName {
		response.Message = "Ownership of the staking address has not been found"
		logger.Error(response.Message)
		return response, generateError(403, "CLM004", response.Message)
	}

	currentPhaseConfig, err := getPhaseConfig(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error while initializing phase config: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM005", response.Message)
	}

	response, claimableAmounAfterDeductingFee, err := claimHelper(ctx, stakingAddr, defaultWalletAddress, response, currentPhaseConfig, bigFee)
	if err != nil {
		return response, err
	}
	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, claimableAmounAfterDeductingFee)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM012", response.Message)
	}

	return response, err
}

func claimHelper(ctx contractapi.TransactionContextInterface, stakingAddr string, defaultWalletAddress string, response *Response, currentPhaseConfig *PhaseConfig, bigFee *big.Int) (*Response, *big.Int, error) {
	stakingReward, err := countStakingReward(ctx, stakingAddr)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while counting staking reward: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM006", response.Message)
	}
	logger.Infof("staking reward counted from countStakingReward func %s", stakingReward.String())

	stakingInfoAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("info~%s", stakingAddr))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching staking details: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM007", response.Message)
	}
	var stakingInfo StakingInfo
	_ = json.Unmarshal(stakingInfoAsBytes, &stakingInfo)

	bigClaimedAmount, _ := new(big.Int).SetString(stakingInfo.Claimed, 10)
	logger.Info("Amout user claimed already fetching from staking info %s", bigClaimedAmount.String())
	claimableAmount := new(big.Int).Set(stakingReward).Sub(stakingReward, bigClaimedAmount)
	logger.Infof("claimable amout %s after deducting claimed amout %s from reward %s", claimableAmount.String(), bigClaimedAmount.String(), stakingReward.String())

	if claimableAmount.Cmp(bigZero) == -1 {
		response.Message = "Failed to claim as current phase is changing, please try again after some time"
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM008", response.Message)
	}

	claimableAmounAfterDeductingFee := new(big.Int).Set(claimableAmount).Sub(claimableAmount, bigFee)
	logger.Infof("claimable amout after deducting fee %s is %s", bigFee.String(), claimableAmounAfterDeductingFee.String())
	err = addClaimUTXO(ctx, defaultWalletAddress, stakingAddr, claimableAmounAfterDeductingFee, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while adding reward utxo: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM009", response.Message)
	}
	bigClaimedAmount = bigClaimedAmount.Add(bigClaimedAmount, claimableAmount)
	bigCurrentStakingAmount, _ := new(big.Int).SetString(stakingInfo.StakedCoins, 10)
	bigCurrentStakingLimit, _ := new(big.Int).SetString(currentPhaseConfig.CurrentStakingLimit, 10)
	stakingInfo.Claimed = bigClaimedAmount.String()
	stakingInfo.StakedCoins = currentPhaseConfig.CurrentStakingLimit
	stakingInfoAsBytes, _ = json.Marshal(stakingInfo)
	err = ctx.GetStub().PutState(fmt.Sprintf("info~%s", stakingAddr), stakingInfoAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating staking details: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM010", response.Message)
	}
	logger.Infof("staking reward before returning response ", stakingReward.String())
	stakingInfo.TotalReward = stakingReward.String()
	stakingInfo.Claimed = claimableAmount.String()

	amounOtherThenStakingLimit := bigCurrentStakingAmount.Sub(bigCurrentStakingAmount, bigCurrentStakingLimit)
	logger.Infof("amounOtherThenStakingLimit: %s", amounOtherThenStakingLimit.String())
	err = transferHelper(ctx, stakingAddr, defaultWalletAddress, amounOtherThenStakingLimit, BUSY_COIN_SYMBOL, bigZero)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while transferring from staking address to default wallet: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "CLM011", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultWalletAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: bigFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, bigZero, generateError(500, "BAL001", response.Message)
	}
	response.Message = "Request to claim staking reward has been successfully accepted"
	response.Success = true
	response.Data = stakingInfo
	logger.Info(response.Message)
	return response, claimableAmounAfterDeductingFee, nil
}

func (bt *Busy) ClaimAll(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	commonName, _ := getCommonName(ctx)
	fee, _ := getCurrentTxFee(ctx)
	bigFee, _ := new(big.Int).SetString(fee, 10)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM001", response.Message)
	}
	currentPhaseConfig, err := getPhaseConfig(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error while initializing phase config: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM005", response.Message)
	}

	var queryString string = fmt.Sprintf(`{
		"selector": {
			"userId": "%s",
			"docType": "stakingAddr"
		 } 
	}`, commonName)
	resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM001", response.Message)
	}
	defer resultIterator.Close()

	// isStakingAddresses to check that atleast one staking address exists for a specific user
	isStakingAddresses := false
	respData := []interface{}{}

	totalClaimed := new(big.Int).SetUint64(0)
	totalFee := new(big.Int).SetUint64(0)
	totalClaimAmountAfterFee := new(big.Int).SetUint64(0)

	for resultIterator.HasNext() {
		data, _ := resultIterator.Next()
		stakingAddr := Wallet{}
		_ = json.Unmarshal(data.Value, &stakingAddr)
		isStakingAddresses = true

		resp, claimAmountAfterFee, err := claimHelper(ctx, stakingAddr.Address, defaultWalletAddress, response, currentPhaseConfig, bigFee)
		if err != nil {
			response.Message = fmt.Sprintf("Error %s occurred while Claiming for %s", err.Error(), stakingAddr.Address)
			logger.Error(response.Message)
			return response, err
		}
		claimed, _ := new(big.Int).SetString(resp.Data.(StakingInfo).Claimed, 10)
		totalClaimed = totalClaimed.Add(totalClaimed, claimed)
		totalFee = totalFee.Add(totalFee, bigFee)
		totalClaimAmountAfterFee = totalClaimAmountAfterFee.Add(totalClaimAmountAfterFee, claimAmountAfterFee)
		respData = append(respData, resp.Data)
	}

	if !isStakingAddresses {
		response.Message = "No staking address exists to claim"
		logger.Error(response.Message)
		return response, generateError(409, "CLM014", response.Message)
	}

	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, totalClaimAmountAfterFee)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CLM012", response.Message)
	}

	response.Message = "Request to claim all staking rewards has been successfully accepted"
	response.Success = true
	response.Data = map[string]interface{}{
		"stakingList":  respData,
		"totalClaimed": totalClaimed.String(),
		"totalFee":     totalFee.String(),
	}
	logger.Info(response.Message)
	return response, nil
}

func (bt *Busy) FetchStakingAddress(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to Fetch Stakng Address"
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	var queryString string = `{
		"selector": {
			"docType": "stakingAddr"
		 } 
	}`

	resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, fmt.Errorf(response.Message)
	}
	defer resultIterator.Close()

	responseData := []interface{}{}
	for resultIterator.HasNext() {
		tmpData := map[string]interface{}{}
		data, _ := resultIterator.Next()
		stakingAddr := Wallet{}
		_ = json.Unmarshal(data.Value, &stakingAddr)
		tmpData["walletId"] = data.Key
		tmpData["createdData"] = time.Unix(int64(stakingAddr.CreatedAt), 0).Format(time.RFC3339)
		defaultWalletAddress, _ := getDefaultWalletAddress(ctx, stakingAddr.UserID)
		tmpData["createdFrom"] = defaultWalletAddress
		responseData = append(responseData, tmpData)
	}
	response.Data = responseData
	response.Success = true
	response.Message = "Staking Address Successfully fetched"
	return response, nil
}

func (bt *Busy) Unstake(ctx contractapi.TransactionContextInterface, stakingAddr string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	commonName, _ := getCommonName(ctx)
	fee, _ := getCurrentTxFee(ctx)
	bigFee, _ := new(big.Int).SetString(fee, 10)
	defaultWalletAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching wallet %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK001", response.Message)
	}
	err = CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, defaultWalletAddress, BUSY_COIN_SYMBOL)
	if bigFee.Cmp(balance) == 1 {
		response.Message = "There is not enough balance for transaction fee in the wallet"
		logger.Error(response.Message)
		return response, generateError(402, "USTK002", response.Message)
	}
	stakingAddrAsBytes, err := ctx.GetStub().GetState(stakingAddr)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching staking address: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK003", response.Message)
	}
	if stakingAddrAsBytes == nil {
		response.Message = fmt.Sprintf("Staking address %s does not exist", stakingAddr)
		logger.Error(response.Message)
		return response, generateError(404, "USTK004", response.Message)
	}
	var stAddr Wallet
	_ = json.Unmarshal(stakingAddrAsBytes, &stAddr)
	if stAddr.UserID != commonName {
		response.Message = "Ownership of the staking address has not been found"
		logger.Error(response.Message)
		return response, generateError(500, "USTK005", response.Message)
	}

	stakingReward, err := countStakingReward(ctx, stakingAddr)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while counting staking reward: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK006", response.Message)
	}

	stakingInfoAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("info~%s", stakingAddr))
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching staking details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK007", response.Message)
	}
	var stakingInfo StakingInfo
	_ = json.Unmarshal(stakingInfoAsBytes, &stakingInfo)

	bigClaimedAmount, _ := new(big.Int).SetString(stakingInfo.Claimed, 10)
	logger.Infof("Amount %s already claimed by %s", bigClaimedAmount.String(), stakingAddr)
	claimableAmount := new(big.Int).Set(stakingReward).Sub(stakingReward, bigClaimedAmount)
	logger.Infof("claimable amount after dedcuting claimed amount %s from total reward %s is %s", bigClaimedAmount.String(), stakingReward.String(), claimableAmount.String())
	bigStakingAmount, _ := new(big.Int).SetString(stakingInfo.StakedCoins, 10)
	logger.Infof("staking amount for staking address %s is %s it is fetched from staking info", stakingAddr, bigStakingAmount.String())
	fmt.Println(bigZero)
	err = transferHelper(ctx, stakingAddr, defaultWalletAddress, bigStakingAmount, BUSY_COIN_SYMBOL, bigZero)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while transferring from staking address to default wallet: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK008", response.Message)
	}

	claimableAmounAfterDeductingFee := new(big.Int).Set(claimableAmount).Sub(claimableAmount, bigFee)
	err = addUTXO(ctx, defaultWalletAddress, claimableAmounAfterDeductingFee, BUSY_COIN_SYMBOL)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while adding reward utxo: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK009", response.Message)
	}

	bigClaimedAmount = bigClaimedAmount.Add(bigClaimedAmount, claimableAmount)
	stakingInfo.Claimed = bigClaimedAmount.String()
	stakingInfo.StakedCoins = new(big.Int).Set(bigStakingAmount).Add(bigStakingAmount, claimableAmount).String()
	stakingInfo.Unstaked = true
	stakingInfoAsBytes, _ = json.Marshal(stakingInfo)
	err = ctx.GetStub().PutState(fmt.Sprintf("info~%s", stakingAddr), stakingInfoAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating staking details: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTk010", response.Message)
	}
	stakingInfo.TotalReward = stakingReward.String()
	stakingInfo.Claimed = claimableAmount.String()

	err = ctx.GetStub().DelState(stakingAddr)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while deleting staking address: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK011", response.Message)
	}
	_, err = updateTotalStakingAddress(ctx, -1)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating number of total staking addresses: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK012", response.Message)
	}

	err = addTotalSupplyUTXO(ctx, BUSY_COIN_SYMBOL, claimableAmounAfterDeductingFee)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while updating total supply: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "USTK013", response.Message)
	}
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultWalletAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: bigFee.String(),
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while sending the balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}

	response.Message = "Request to unstake staking address has been successfully accepted"
	response.Success = true
	response.Data = stakingInfo
	logger.Info(response.Message)
	return response, nil
}

// GetCurrrentPhase config is to retrieve the current Phase config in BusyChain
func (bt *Busy) GetCurrentPhase(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	currentPhaseConfig, err := getPhaseConfig(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting phase config: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CURP001", response.Message)
	}

	response.Success = true
	response.Message = "Current BusyChain phase has been successfully fetched"
	response.Data = currentPhaseConfig
	return response, nil
}

// GetCurrentFee config is to retrieve the current fees in BusyChain
func (bt *Busy) GetCurrentFee(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// Fetch current transfer fee
	transferFee, err := getCurrentTxFee(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching transfer fee %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "CURF001", response.Message)
	}
	response.Success = true
	response.Message = "Current transfer fee has been successfully fetched"
	response.Data = transferFee
	return response, nil
}

// GetBusyAddress is to fetch the busy address generated during init
func (bt *Busy) GetBusyAddress(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	// Fetch current transfer fee
	commonName, err := getCommonName(ctx)
	if err != nil {
		response.Message = fmt.Sprintf("Error fetching common name %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "GBA001", response.Message)
	}
	if commonName != "busy_network" {
		response.Message = fmt.Sprintf("You are not allowed to get Busy Address %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "GBA002", response.Message)
	}
	address, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while fetching busy Address %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "GBA003", response.Message)
	}
	response.Success = true
	response.Message = "Busy Address successfully fetched"
	response.Data = address
	return response, nil
}

// AuthenticateUser config is to check if user certificate is valid
func (bt *Busy) AuthenticateUser(ctx contractapi.TransactionContextInterface, userID string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}
	// checking if user exists
	userAsBytes, err := ctx.GetStub().GetState(userID)
	if userAsBytes == nil {
		response.Message = "User does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "ATU002", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "ATU003", response.Message)
	}
	commonName, _ := getCommonName(ctx)
	if userID != commonName {
		response.Message = "User does not match with certificate"
		logger.Info(response.Message)
		return response, generateError(409, "ATU004", response.Message)
	}
	response.Success = true
	response.Message = "User successfully Authenticated"
	response.Data = true
	return response, nil
}
