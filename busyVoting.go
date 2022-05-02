package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

// BusyVoting contract
type BusyVoting struct {
	contractapi.Contract
}

func (bv *BusyVoting) CreatePool(ctx contractapi.TransactionContextInterface, walletid string, PoolName string, PoolDescription string, token string) (*Response, error) {
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

	votingConfigBytes, _ := ctx.GetStub().GetState("VotingConfig")
	votingConfig := VotingConfig{}
	if err := json.Unmarshal(votingConfigBytes, &votingConfig); err != nil {
		response.Message = fmt.Sprintf("Error occurred while unmarshalling the voting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "POOL001", response.Message)
	}
	logger.Info("Received a Create Pool Transaction with ", PoolName)

	if PoolName == "" {
		response.Message = "Pool Name cannot be empty"
		logger.Error(response.Message)
		return response, generateError(412, "POOL002", response.Message)
	}

	re := regexp.MustCompile("^[a-zA-Z0-9_ ]*$")
	if len(PoolName) > 30 {
		response.Message = "Pool Name cannot be more than 30 characters"
		logger.Error(response.Message)
		return response, generateError(412, "POOL003", response.Message)
	}

	// checking for special characters
	if !re.MatchString(PoolName) {
		response.Message = "Pool name cannot contain special characters"
		logger.Error(response.Message)
		return response, generateError(412, "POOL004", response.Message)
	}

	if len(PoolDescription) > 500 {
		response.Message = "Pool Description cannot have more than 500 characters"
		logger.Error(response.Message)
		return response, generateError(412, "POOL005", response.Message)
	}
	//  Checking if pool Already Exists
	poolAsBytes, _ := ctx.GetStub().GetState("PoolData")
	if poolAsBytes != nil {
		response.Message = "Running pool already exists, it is not possible to run more than 1 one pool at a time"
		logger.Info(response.Message)
		return response, generateError(409, "POOL006", response.Message)
	}

	commonName, _ := getCommonName(ctx)
	defaultAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = "Error occurred while fetching wallet"
		logger.Error(response.Message)
		return response, generateError(500, "POOL007", response.Message)
	}

	if walletid != defaultAddress {
		response.Message = "Wallet in the request does not match with the wallet"
		logger.Error(response.Message)
		return response, generateError(409, "POOL008", response.Message)
	}
	balance, _ := getBalanceHelper(ctx, defaultAddress, BUSY_COIN_SYMBOL)

	minimumCoins, _ := new(big.Int).SetString(votingConfig.MinimumCoins, 10)
	if balance.Cmp(minimumCoins) == -1 {
		response.Message = "You do not have the minimum number of coins to create new voting pool"
		//response.Message = fmt.Sprintf("User %s does not have the minimum number of %s millions of coins to create a new pool", commonName, votingConfig.MinimumCoins)
		logger.Error(response.Message)
		return response, generateError(402, "POOL009", response.Message)
	}

	err = burnCoins(ctx, defaultAddress, votingConfig.PoolFee, token)

	if err != nil {
		response.Message = fmt.Sprintf("Error while burning tokens at pool creation %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "POOL010", response.Message)
	}

	poolData := Pool{
		DocType:          "pool",
		CreatedBy:        defaultAddress,
		PoolID:           response.TxID,
		CreatedAt:        time.Now(),
		VotingStartAt:    time.Now().Add(votingConfig.VotingStartTime),
		VotingEndAt:      time.Now().Add(votingConfig.VotingPeriod),
		VotingAddressYes: "Yes-" + response.TxID,
		VotingAddressNo:  "No-" + response.TxID,
		VotingPowerYes:   bigZero.String(),
		VotingPowerNo:    bigZero.String(),
		TokenType:        token,
		PoolName:         PoolName,
		PoolDescription:  PoolDescription,
		PoolFee:          votingConfig.PoolFee,
	}
	poolAddrAsBytes, _ := json.Marshal(poolData)

	// storing the data at PoolData
	err = ctx.GetStub().PutState("PoolData", poolAddrAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "POOL011", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: votingConfig.PoolFee,
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	response.Success = true
	response.Data = poolData
	response.Message = "Pool Created Successfully"
	return response, nil
}

func (bv *BusyVoting) CreateVote(ctx contractapi.TransactionContextInterface, walletid string, votingaddress string, amount string, voteType string, token string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while Validating default credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	//  Checking if pool Already Exists
	poolAsBytes, err := ctx.GetStub().GetState("PoolData")
	if poolAsBytes == nil || err != nil {
		response.Message = "Voting pool does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "VOTE001", response.Message)
	}
	PoolData := Pool{}

	if err = json.Unmarshal(poolAsBytes, &PoolData); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the user state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VOTE002", response.Message)
	}

	if voteType != "Yes" && voteType != "No" {
		response.Message = "Invalid VoteType provided"
		logger.Error(response.Message)
		return response, generateError(412, "VOTE003", response.Message)
	}

	if PoolData.VotingAddressYes != fmt.Sprintf("Yes-%s", votingaddress) && PoolData.VotingAddressNo != fmt.Sprintf("No-%s", votingaddress) {
		response.Message = "Voting address does not exists"
		logger.Error(response.Message)
		return response, generateError(404, "VOTE004", response.Message)
	}

	// checking if voting has started
	if time.Since(PoolData.VotingStartAt) < 0 {
		response.Message = fmt.Sprintf("You can not vote now. The voting starts on %s UTC", PoolData.VotingStartAt.Format("2006-01-02 15:04:05"))
		logger.Error(response.Message)
		return response, generateError(405, "VOTE005", response.Message)
	}

	// checking if voting has ended
	if time.Since(PoolData.VotingEndAt) > 0 {
		response.Message = fmt.Sprintf("Voting has ended at %s UTC", PoolData.VotingEndAt.Format("2006-01-02 15:04:05"))
		logger.Error(response.Message)
		return response, generateError(405, "VOTE006", response.Message)
	}

	commonName, _ := getCommonName(ctx)
	defaultAddress, err := getDefaultWalletAddress(ctx, commonName)
	if err != nil {
		response.Message = "Error getting the default address"
		logger.Error(response.Message)
		return response, generateError(500, "VOTE007", response.Message)
	}
	if walletid != defaultAddress {
		response.Message = "Walletid in the request does not match with default wallet id"
		logger.Error(response.Message)
		return response, generateError(400, "VOTE008", response.Message)
	}

	balance, _ := getBalanceHelper(ctx, defaultAddress, BUSY_COIN_SYMBOL)

	amountInt, isConverted := new(big.Int).SetString(amount, 10)

	if !isConverted {
		response.Message = "Invalid Amount provided in the request"
		logger.Error(response.Message)
		return response, generateError(412, "VOTE009", response.Message)
	}

	if amountInt.Cmp(bigZero) <= 0 {
		response.Message = "Amount to vote cannot be zero or negative"
		logger.Error(response.Message)
		return response, generateError(412, "VOTE010", response.Message)
	}

	if balance.Cmp(amountInt) == -1 {
		response.Message = "User does not have enough coins to vote"
		logger.Error(response.Message)
		return response, generateError(402, "VOTE011", response.Message)
	}
	err = burnCoins(ctx, defaultAddress, amount, token)

	if err != nil {
		response.Message = fmt.Sprintf("Error while burning tokens at vote %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VOTE012", response.Message)
	}

	if voteType == "Yes" {
		votingPowerYay, _ := new(big.Int).SetString(PoolData.VotingPowerYes, 10)
		votingPowerYay = new(big.Int).Add(amountInt, votingPowerYay)
		PoolData.VotingPowerYes = votingPowerYay.String()
	} else {
		VotingPowerNay, _ := new(big.Int).SetString(PoolData.VotingPowerNo, 10)
		VotingPowerNay = new(big.Int).Add(amountInt, VotingPowerNay)
		PoolData.VotingPowerNo = VotingPowerNay.String()
	}
	PoolDataBytes, _ := json.Marshal(PoolData)
	vote := Vote{
		DocType:     "Vote",
		VoteTime:    time.Now(),
		VoteAddress: votingaddress,
		Tokens:      amount,
		VoteType:    voteType,
	}

	VoteListAsBytes, _ := ctx.GetStub().GetState(votingaddress)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VOTE013", response.Message)
	}
	voteList := []Vote{}

	_ = json.Unmarshal(VoteListAsBytes, &voteList)
	voteList = append(voteList, vote)

	VoteListAsBytes, _ = json.Marshal(voteList)
	// storing the data of votelist at the vote Address
	err = ctx.GetStub().PutState(votingaddress, VoteListAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VOTE014", response.Message)
	}

	if err = ctx.GetStub().PutState("PoolData", PoolDataBytes); err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "VOTE014", response.Message)
	}

	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: defaultAddress,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
		TransactionFee: amount,
		TransactionId:  response.TxID,
	}
	balanceAsBytes, _ := json.Marshal(balanceData)
	err = ctx.GetStub().SetEvent(BALANCE_EVENT, balanceAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while Sending the Balance event: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "BAL001", response.Message)
	}
	response.Success = true
	response.Message = "Voted successfully"
	return response, nil
}

func (bv *BusyVoting) DestroyPool(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to Delete Voting pool"
		logger.Error(response.Message)
		return response, generateError(403, "DPOL001", response.Message)
	}

	//  Checking if pool doesnot Exists
	poolAsBytes, err := ctx.GetStub().GetState("PoolData")

	if poolAsBytes == nil || err != nil {
		response.Message = "Voting pool does not exist"
		logger.Error(response.Message)
		return response, generateError(404, "DPOL002", response.Message)
	}
	PoolData := Pool{}

	if err := json.Unmarshal(poolAsBytes, &PoolData); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the user state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "DPOL003", response.Message)
	}
	// checking if voting has started
	if time.Since(PoolData.VotingEndAt) < 0 {
		response.Message = fmt.Sprintf("Voting will end at %s UTC", PoolData.VotingEndAt.Format("2006-01-02 15:04:05"))
		logger.Error(response.Message)
		return response, generateError(400, "DPOL004", response.Message)
	}

	poolDataListAsBytes, err := ctx.GetStub().GetState("PoolDataList")
	if err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the pool List: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "DPOL005", response.Message)
	}
	poolDataList := []Pool{}
	_ = json.Unmarshal(poolDataListAsBytes, &poolDataList)
	// appending the current pool data
	poolDataList = append(poolDataList, PoolData)
	poolAddrListAsBytes, _ := json.Marshal(poolDataList)
	// storing the data at PoolDataList
	err = ctx.GetStub().PutState("PoolDataList", poolAddrListAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "DPOL006", response.Message)
	}

	if err := ctx.GetStub().DelState("PoolData"); err != nil {
		response.Message = "Error deleting the voting pool"
		logger.Error(response.Message)
		return response, generateError(500, "DPOL007", response.Message)
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
	response.Success = true
	response.Message = "Pool Destroyed Successfully"
	return response, nil
}

func (bv *BusyVoting) QueryPool(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to query voting pool"
		logger.Error(response.Message)
		return response, generateError(403, "QUP001", response.Message)
	}

	//  Checking if pool Already Exists
	poolAsBytes, err := ctx.GetStub().GetState("PoolData")
	if poolAsBytes == nil || err != nil {
		response.Message = "Voting pool does not exist"
		logger.Info(response.Message)
		return response, generateError(404, "QUP002", response.Message)
	}
	PoolData := Pool{}

	if err := json.Unmarshal(poolAsBytes, &PoolData); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the user state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "QUP003", response.Message)
	}
	response.Data = PoolData
	response.Success = true
	response.Message = "Pool data has been successfully fetched"
	return response, nil
}

// burnCoins is to burn some coins from user and reduce total supply accordingly for voting functionity
func burnCoins(ctx contractapi.TransactionContextInterface, address string, coins string, token string) error {
	minusOne, _ := new(big.Int).SetString("-1", 10)
	bigTxFee, _ := new(big.Int).SetString(coins, 10)
	err := addTotalSupplyUTXO(ctx, token, new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee))
	if err != nil {
		return err
	}

	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  bigTxFee.Mul(bigTxFee, minusOne).String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err = ctx.GetStub().PutState(fmt.Sprintf("voting~%s~%s~%s", ctx.GetStub().GetTxID(), address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}

// Pool History to retrieve the List of pools created till date
func (bv *BusyVoting) PoolHistory(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	poolDataListAsBytes, err := ctx.GetStub().GetState("PoolDataList")
	if err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the pool List: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "PHIS001", response.Message)
	}
	poolDataList := []Pool{}
	_ = json.Unmarshal(poolDataListAsBytes, &poolDataList)

	response.Success = true
	response.Data = poolDataList
	response.Message = "Pool history data has been successfully fetched"
	return response, nil
}

// Pool Config to retrieve the configuration date for the Pool
func (bv *BusyVoting) PoolConfig(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	votingConfigBytes, err := ctx.GetStub().GetState("VotingConfig")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while retrieving the voting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "PCNF001", response.Message)
	}
	votingConfig := VotingConfig{}
	if err = json.Unmarshal(votingConfigBytes, &votingConfig); err != nil {
		response.Message = fmt.Sprintf("Error occurred while unmarshalling the voting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "PCNF002", response.Message)
	}
	response.Success = true
	response.Data = votingConfig
	response.Message = "Voting pool configuration has been successfully fetched"
	return response, nil
}

// Update Pool Config to Update the configuration date for the Pool
func (bv *BusyVoting) UpdatePoolConfig(ctx contractapi.TransactionContextInterface, MinimumCoins string, PoolFee string, VotingPeriod int64, VotingStartTime int64) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}
	votingConfig := VotingConfig{
		MinimumCoins:    MinimumCoins,
		PoolFee:         PoolFee,
		VotingPeriod:    time.Duration(VotingPeriod),
		VotingStartTime: time.Duration(VotingStartTime),
	}
	votingConfigBytes, err := json.Marshal(&votingConfig)
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while marshalling the voting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UPCN001", response.Message)
	}

	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed to update Voting Pool config"
		logger.Error(response.Message)
		return response, generateError(403, "UPCN002", response.Message)
	}

	err = ctx.GetStub().PutState("VotingConfig", votingConfigBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UPCN003", response.Message)
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
	response.Success = true
	response.Data = votingConfig
	response.Message = "Voting Config Updated successfully"
	return response, nil
}
