package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

var bigOne *big.Int = new(big.Int).SetUint64(1)
var bigTwo *big.Int = new(big.Int).SetUint64(2)
var minusOne *big.Int = new(big.Int).SetInt64(-1)

const (
	PHASE_UPDATE_TIMELINE = "phaseUpdateTimeline"
	REWARD_NUMERATOR      = "1045706897862"
	REWARD_DENOMINATOR    = "100000000000000000000"
	BUSY_COIN_SYMBOL      = "BUSY"
	TOTAL_SUPPLY_KEY      = "TOTAL_SUPPLY"
)

// UnknownTransactionHandler returns a shim error with details of a bad transaction request
func UnknownTransactionHandler(ctx contractapi.TransactionContextInterface) error {
	fcn, args := ctx.GetStub().GetFunctionAndParameters()
	return fmt.Errorf("invalid function %s passed with args %v", fcn, args)
}

func getCommonName(ctx contractapi.TransactionContextInterface) (string, error) {
	x509, err := ctx.GetClientIdentity().GetX509Certificate()
	if err != nil {
		return "", err
	}
	return x509.Subject.CommonName, nil
}

func pruneUTXOs(ctx contractapi.TransactionContextInterface, sender string, token string) (*big.Int, []string, error) {
	// Query all the records where owner is sender and token is specified token

	var utxo UTXO
	balance, _ := new(big.Int).SetString("0", 10)
	var queryString string = fmt.Sprintf(`{
		"selector": {
		   "address": "%s",
		   "token": "%s"
		}
	}`, sender, token)
	resultIterator, err := ctx.GetStub().GetQueryResult(queryString)
	if err != nil {
		return balance, nil, err
	}
	defer resultIterator.Close()

	// Loop through all the fetched records and sum all of their amount delete all existing utxo records
	var utxoKeys []string
	for resultIterator.HasNext() {
		data, _ := resultIterator.Next()
		_ = json.Unmarshal(data.Value, &utxo)
		utxoKeys = append(utxoKeys, data.Key)
		if err != nil {
			return balance, nil, err
		}
		bigAmount, _ := new(big.Int).SetString(utxo.Amount, 10)
		balance = balance.Add(balance, bigAmount)
	}
	return balance, utxoKeys, nil
}

func transferHelper(ctx contractapi.TransactionContextInterface, sender string, recipiant string, amount *big.Int, token string, fee *big.Int) error {
	logger.Infof("In transfer helper \n sender %s \n recipiant %s \n amount %s \n tokne %s \n fee %s \n ", sender, recipiant, amount.String(), token, fee.String())
	var txID string = ctx.GetStub().GetTxID()

	if amount.String() == "0" {
		return nil
	}

	if token == BUSY_COIN_SYMBOL {
		// Prune exsting utxo of sender and count his balance
		balance, utxoKeys, err := pruneUTXOs(ctx, sender, token)
		if err != nil {
			return fmt.Errorf("error while pruning UTXOs: %s", err.Error())
		}
		logger.Infof("balance of sender after prune utxo %s", balance.String())

		bigAmountWithTransferFee := new(big.Int).Set(fee).Add(fee, amount)
		logger.Infof("bigAmountWithTransferFee: %s", bigAmountWithTransferFee)

		// Check if sender has enough balance
		if bigAmountWithTransferFee.Cmp(balance) == 1 {
			return fmt.Errorf("amount %s higher then your total balance %s", amount.String(), balance.String())
		}

		// Delete existing utxos
		for _, v := range utxoKeys {
			_ = ctx.GetStub().DelState(v)
		}
		// Deduct balance of sender
		balance = balance.Sub(balance, bigAmountWithTransferFee)
		utxo := UTXO{
			DocType: "utxo",
			Address: sender,
			Amount:  balance.String(),
			Token:   token,
		}
		utxoAsBytes, _ := json.Marshal(utxo)
		_ = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", txID, sender, recipiant, token), utxoAsBytes)

		// Create new utxo for recipiant
		utxo = UTXO{
			DocType: "utxo",
			Address: recipiant,
			Amount:  amount.String(),
			Token:   token,
		}
		utxoAsBytes, _ = json.Marshal(utxo)
		err = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", txID, recipiant, sender, token), utxoAsBytes)
		if err != nil {
			return fmt.Errorf("error while put state in ledger: %s", err.Error())
		}
		return nil
	} else {
		// Prune exsting utxo of sender and count his balance
		tokenBalance, tokenUtxoKeys, err := pruneUTXOs(ctx, sender, token)
		if err != nil {
			return fmt.Errorf("error while pruning token UTXOs: %s", err.Error())
		}
		busyBalance, busyUtxoKeys, err := pruneUTXOs(ctx, sender, BUSY_COIN_SYMBOL)
		if err != nil {
			return fmt.Errorf("error while pruning busy UTXOs: %s", err.Error())
		}

		// Check if sender has enough balance
		if fee.Cmp(busyBalance) == 1 {
			return fmt.Errorf("amount %s higher then your total balance %s", fee.String(), busyBalance.String())
		}
		if amount.Cmp(tokenBalance) == 1 {
			return fmt.Errorf("amount %s higher then your total balance %s", amount.String(), tokenBalance.String())
		}

		// Delete existing utxos
		for _, v := range tokenUtxoKeys {
			_ = ctx.GetStub().DelState(v)
		}
		for _, v := range busyUtxoKeys {
			_ = ctx.GetStub().DelState(v)
		}
		// Deduct balance of sender
		tokenBalance = tokenBalance.Sub(tokenBalance, amount)
		utxo := UTXO{
			DocType: "utxo",
			Address: sender,
			Amount:  tokenBalance.String(),
			Token:   token,
		}
		utxoAsBytes, _ := json.Marshal(utxo)
		_ = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", txID, sender, recipiant, token), utxoAsBytes)

		// Create new utxo for recipiant
		utxo = UTXO{
			DocType: "utxo",
			Address: recipiant,
			Amount:  amount.String(),
			Token:   token,
		}
		utxoAsBytes, _ = json.Marshal(utxo)
		err = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", txID, recipiant, sender, token), utxoAsBytes)
		if err != nil {
			return fmt.Errorf("error while put state in ledger: %s", err.Error())
		}

		busyBalance = busyBalance.Sub(busyBalance, fee)
		// deduct tx fee from sender
		utxo = UTXO{
			DocType: "utxo",
			Address: sender,
			Amount:  busyBalance.String(),
			Token:   BUSY_COIN_SYMBOL,
		}
		utxoAsBytes, _ = json.Marshal(utxo)
		err = ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", txID, recipiant, sender, BUSY_COIN_SYMBOL), utxoAsBytes)
		if err != nil {
			return fmt.Errorf("error while put state in ledger: %s", err.Error())
		}
		return nil
	}
}

func getBalanceHelper(ctx contractapi.TransactionContextInterface, address string, token string) (*big.Int, error) {

	walletAsBytes, err := ctx.GetStub().GetState(address)
	if err != nil {
		return bigZero, fmt.Errorf("error while fetching wallet: %s", err.Error())
	}
	if walletAsBytes == nil {
		return bigZero, fmt.Errorf("address %s not found", address)
	}
	balance, _, err := pruneUTXOs(ctx, address, token)
	if err != nil {
		return bigZero, fmt.Errorf("error while fetching balance: %s", err.Error())
	}
	return balance, nil
}

func getDefaultWalletAddress(ctx contractapi.TransactionContextInterface, commonName string) (string, error) {
	userAsBytes, err := ctx.GetStub().GetState(commonName)
	if err != nil {
		return "", fmt.Errorf("error while fetching user details")
	}
	if userAsBytes == nil {
		return "", fmt.Errorf("user doesn't exists")
	}
	var user User
	_ = json.Unmarshal(userAsBytes, &user)
	return user.DefaultWallet, nil
}

func addUTXO(ctx contractapi.TransactionContextInterface, address string, amount *big.Int, symbol string) error {
	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  amount.String(),
		Token:   symbol,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err := ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s", ctx.GetStub().GetTxID(), address, symbol), utxoAsBytes)
	return err
}

func addClaimUTXO(ctx contractapi.TransactionContextInterface, address string, stakingAddr string, amount *big.Int, symbol string) error {
	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  amount.String(),
		Token:   symbol,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err := ctx.GetStub().PutState(fmt.Sprintf("%s~%s~%s~%s", ctx.GetStub().GetTxID(), address, symbol, stakingAddr), utxoAsBytes)
	return err
}

func calculatePercentage(amount *big.Int, numerator uint64, denominator uint64) (*big.Int, error) {
	bigNumerator := new(big.Int).SetUint64(numerator)
	bigDenominator := new(big.Int).SetUint64(denominator)

	if bigNumerator.Cmp(bigZero) == 0 {
		return nil, errors.New("numerator cannot be zero")
	}

	if bigDenominator.Cmp(bigZero) == 0 {
		return nil, errors.New("denominator cannot be zero")
	}
	if bigNumerator.Cmp(bigDenominator) > 0 {
		return nil, errors.New("numerator cannot be greater than denominator")
	}
	amount = amount.Mul(amount, bigNumerator)
	return amount.Div(amount, bigDenominator), nil
}

// last Message key
func getLastMessageKey(userId string) string {
	return fmt.Sprintf("lastmessage%s", userId)
}

func addTotalSupplyUTXO(ctx contractapi.TransactionContextInterface, tokenSymbol string, amount *big.Int) error {
	err := addUTXO(ctx, TOTAL_SUPPLY_KEY, amount, tokenSymbol)
	if err != nil {
		return err
	}
	return nil
}

// burnTxFeeWithTotalSupply burn tx fee from user and reduce total supply accordingly
func burnTxFeeWithTotalSupply(ctx contractapi.TransactionContextInterface, address string, token string) error {
	txFee, err := getCurrentTxFee(ctx)
	if err != nil {
		return err
	}
	minusOne, _ := new(big.Int).SetString("-1", 10)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)
	err = addTotalSupplyUTXO(ctx, token, new(big.Int).Set(bigTxFee).Mul(minusOne, bigTxFee))
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
	err = ctx.GetStub().PutState(fmt.Sprintf("burnTxFee~%s~%s~%s", ctx.GetStub().GetTxID(), address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}

// burnTxFee to burn tx fee from the user address
func burnTxFee(ctx contractapi.TransactionContextInterface, address string, token string) error {
	txFee, err := getCurrentTxFee(ctx)
	if err != nil {
		return err
	}
	minusOne, _ := new(big.Int).SetString("-1", 10)
	bigTxFee, _ := new(big.Int).SetString(txFee, 10)

	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  bigTxFee.Mul(bigTxFee, minusOne).String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err = ctx.GetStub().PutState(fmt.Sprintf("burnTxFee~%s~%s~%s", ctx.GetStub().GetTxID(), address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}

// getCurrentTxFee get current tx fee from blockchain
func getCurrentTxFee(ctx contractapi.TransactionContextInterface) (string, error) {
	return TRANSFER_FEE, nil
}

func updatePhase(ctx contractapi.TransactionContextInterface) (*PhaseConfig, error) {
	phaseConfigAsBytes, err := ctx.GetStub().GetState("phaseConfig")
	if err != nil {
		return nil, err
	}
	if phaseConfigAsBytes == nil {
		return nil, fmt.Errorf("initialize chaincode first")
	}

	var phaseConfig PhaseConfig
	_ = json.Unmarshal(phaseConfigAsBytes, &phaseConfig)
	bigCurrentStakingAddr, _ := new(big.Int).SetString(phaseConfig.TotalStakingAddr, 10)
	bigCurrentStakingAddr = bigCurrentStakingAddr.Add(bigCurrentStakingAddr, bigOne)
	phaseConfig.TotalStakingAddr = bigCurrentStakingAddr.String()
	if bigCurrentStakingAddr.String() == phaseConfig.NextStakingAddrTarget {
		phaseConfig.CurrentPhase += 1

		bigNextStakingAddrTarget, _ := new(big.Int).SetString(phaseConfig.NextStakingAddrTarget, 10)
		bigNextStakingAddrTarget = bigNextStakingAddrTarget.Mul(bigNextStakingAddrTarget, bigTwo)
		phaseConfig.NextStakingAddrTarget = bigNextStakingAddrTarget.String()

		bigCurrentStakingLimit, _ := new(big.Int).SetString(phaseConfig.CurrentStakingLimit, 10)
		bigCurrentStakingLimit = bigCurrentStakingLimit.Div(bigCurrentStakingLimit, bigTwo)
		phaseConfig.CurrentStakingLimit = bigCurrentStakingLimit.String()
	}
	phaseConfigAsBytes, _ = json.Marshal(phaseConfig)
	err = ctx.GetStub().PutState("phaseConfig", phaseConfigAsBytes)
	if err != nil {
		return nil, err
	}

	phaseUpdateTimeline := map[uint64]PhaseUpdateInfo{}
	phaseUpdateTimelineAsBytes, _ := ctx.GetStub().GetState(PHASE_UPDATE_TIMELINE)
	_ = json.Unmarshal(phaseUpdateTimelineAsBytes, &phaseUpdateTimeline)
	now, _ := ctx.GetStub().GetTxTimestamp()
	phaseUpdateTimeline[phaseConfig.CurrentPhase] = PhaseUpdateInfo{
		UpdatedAt:    uint64(now.Seconds),
		StakingLimit: phaseConfig.CurrentStakingLimit,
	}
	phaseUpdateTimelineAsBytes, _ = json.Marshal(phaseUpdateTimeline)
	err = ctx.GetStub().PutState(PHASE_UPDATE_TIMELINE, phaseUpdateTimelineAsBytes)
	return &phaseConfig, err
}

func getPhaseConfig(ctx contractapi.TransactionContextInterface) (*PhaseConfig, error) {
	phaseConfigAsBytes, err := ctx.GetStub().GetState("phaseConfig")
	if err != nil {
		return nil, err
	}
	if phaseConfigAsBytes == nil {
		return nil, fmt.Errorf("initialize chaincode first")
	}
	var phaseConfig PhaseConfig
	_ = json.Unmarshal(phaseConfigAsBytes, &phaseConfig)
	return &phaseConfig, nil
}

func ifTokenExists(ctx contractapi.TransactionContextInterface, tokenSymbol string) (bool, error) {
	tokenAsBytes, err := ctx.GetStub().GetState(generateTokenStateAddress(tokenSymbol))
	if err != nil {
		return false, err
	}
	if tokenAsBytes == nil {
		return false, nil
	}
	return true, nil
}

func generateTokenStateAddress(symbol string) string {
	// symbol is case insensitive
	symbol = strings.ToUpper(symbol)
	return "B-" + tokenAddressPrefix + base64Encode(fmt.Sprintf("token-busy20-%s", symbol))
}
func countStakingReward(ctx contractapi.TransactionContextInterface, stakingAddr string) (*big.Int, error) {
	now, _ := ctx.GetStub().GetTxTimestamp()
	bigRewardNumerator, _ := new(big.Int).SetString(REWARD_NUMERATOR, 10)
	bigRewardDenominator, _ := new(big.Int).SetString(REWARD_DENOMINATOR, 10)
	currentPhaseConfig, err := getPhaseConfig(ctx)
	if err != nil {
		return nil, err
	}

	stakingInfo, err := getStakingInfo(ctx, stakingAddr)
	if err != nil {
		return nil, err
	}

	phaseUpdateTimeline, err := getPhaseUpdateTimeline(ctx)
	if err != nil {
		return nil, err
	}

	logger.Infof("counting staking reward for address %s with current time %s", stakingAddr, strconv.Itoa(int(now.Seconds)))
	if stakingInfo.Phase == currentPhaseConfig.CurrentPhase {
		logger.Infof("user created staking address in phase %d and current phase is %d means user is claiming in same phase", stakingInfo.Phase, currentPhaseConfig.CurrentPhase)
		stakingTimePeriod := uint64(now.Seconds) - stakingInfo.TimeStamp
		bigStakingTimePeriod := new(big.Int).SetUint64(stakingTimePeriod)
		logger.Infof("stakingTimePeriod: %s", bigStakingTimePeriod.String())
		percentageNumerator := bigStakingTimePeriod.Mul(bigStakingTimePeriod, bigRewardNumerator)
		logger.Infof("percentageNumerator: %s", percentageNumerator.String())
		bigStakingAmount, _ := new(big.Int).SetString(phaseUpdateTimeline[stakingInfo.Phase].StakingLimit, 10)
		logger.Infof("stakingAmount: %s", bigStakingAmount.String())
		tmpStakingReward := bigStakingAmount.Mul(bigStakingAmount, percentageNumerator)
		logger.Infof("tmpStakingReward: %s", tmpStakingReward.String())
		stakingReward := tmpStakingReward.Div(tmpStakingReward, bigRewardDenominator)
		logger.Infof("stakingReward: %s", stakingReward.String())
		return stakingReward, err
	}
	var phaseCount uint64 = stakingInfo.Phase
	var reward *big.Int = new(big.Int).Set(bigZero)
	logger.Infof("user created staking address in phase %d and current phase is %d means user is not claiming in same phase", stakingInfo.Phase, currentPhaseConfig.CurrentPhase)
	for phaseCount != currentPhaseConfig.CurrentPhase+1 {
		logger.Infof("#################### loop starting with phase %d and current reward is %s ####################", phaseCount, reward.String())
		if phaseCount == stakingInfo.Phase {
			logger.Info("##### couting reward for same phase in which user created staking address phase: %d", phaseCount)
			stakingTimePeriod := phaseUpdateTimeline[phaseCount+1].UpdatedAt - stakingInfo.TimeStamp
			bigStakingTimePeriod := new(big.Int).SetUint64(stakingTimePeriod)
			logger.Infof("stakingTimePeriod: %s", bigStakingTimePeriod.String())
			percentageNumerator := bigStakingTimePeriod.Mul(bigStakingTimePeriod, bigRewardNumerator)
			logger.Infof("percentageNumerator: %s", percentageNumerator.String())
			bigStakingAmount, _ := new(big.Int).SetString(phaseUpdateTimeline[phaseCount].StakingLimit, 10)
			logger.Infof("stakingAmount: %s", bigStakingAmount.String())
			tmpStakingReward := bigStakingAmount.Mul(bigStakingAmount, percentageNumerator)
			logger.Infof("tmpStakingReward: %s", tmpStakingReward.String())
			stakingReward := tmpStakingReward.Div(tmpStakingReward, bigRewardDenominator)
			reward = reward.Add(reward, stakingReward)
			logger.Infof("stakingReward after couting reward for phase %d: %s", phaseCount, stakingReward.String())
		} else if phaseCount == currentPhaseConfig.CurrentPhase {
			logger.Info("##### couting reward for current phase: %d", phaseCount)
			stakingTimePeriod := uint64(now.Seconds) - phaseUpdateTimeline[phaseCount-1].UpdatedAt
			bigStakingTimePeriod := new(big.Int).SetUint64(stakingTimePeriod)
			logger.Infof("stakingTimePeriod: %s", bigStakingTimePeriod.String())
			percentageNumerator := bigStakingTimePeriod.Mul(bigStakingTimePeriod, bigRewardNumerator)
			logger.Infof("percentageNumerator: %s", percentageNumerator.String())
			bigStakingAmount, _ := new(big.Int).SetString(phaseUpdateTimeline[phaseCount].StakingLimit, 10)
			logger.Infof("stakingAmount: %s", bigStakingAmount.String())
			tmpStakingReward := bigStakingAmount.Mul(bigStakingAmount, percentageNumerator)
			logger.Infof("tmpStakingReward: %s", tmpStakingReward.String())
			stakingReward := tmpStakingReward.Div(tmpStakingReward, bigRewardDenominator)
			reward = reward.Add(reward, stakingReward)
			logger.Infof("stakingReward after couting reward for phase %d: %s", phaseCount, stakingReward.String())
		} else {
			logger.Info("##### couting reward for phase: %d", phaseCount)
			stakingTimePeriod := phaseUpdateTimeline[phaseCount+1].UpdatedAt - phaseUpdateTimeline[phaseCount].UpdatedAt
			bigStakingTimePeriod := new(big.Int).SetUint64(stakingTimePeriod)
			logger.Infof("stakingTimePeriod: %s", bigStakingTimePeriod.String())
			percentageNumerator := bigStakingTimePeriod.Mul(bigStakingTimePeriod, bigRewardNumerator)
			logger.Infof("percentageNumerator: %s", percentageNumerator.String())
			bigStakingAmount, _ := new(big.Int).SetString(phaseUpdateTimeline[phaseCount].StakingLimit, 10)
			logger.Infof("stakingAmount: %s", bigStakingAmount.String())
			tmpStakingReward := bigStakingAmount.Mul(bigStakingAmount, percentageNumerator)
			logger.Infof("tmpStakingReward: %s", tmpStakingReward.String())
			stakingReward := tmpStakingReward.Div(tmpStakingReward, bigRewardDenominator)
			reward = reward.Add(reward, stakingReward)
			logger.Infof("stakingReward after couting reward for phase %d: %s", phaseCount, stakingReward.String())
		}
		phaseCount += 1
		logger.Infof("#################### loop ended with phase %d and current reward is %s ####################", phaseCount, reward.String())
	}
	logger.Infof("After finishing all iteration of loop reward is %s", reward.String())
	return reward, err
}

func getStakingInfo(ctx contractapi.TransactionContextInterface, stakingAddr string) (*StakingInfo, error) {
	stakingInfoAsBytes, err := ctx.GetStub().GetState(fmt.Sprintf("info~%s", stakingAddr))
	if err != nil {
		return nil, err
	}
	var stakingInfo StakingInfo
	_ = json.Unmarshal(stakingInfoAsBytes, &stakingInfo)
	return &stakingInfo, nil
}

func getPhaseUpdateTimeline(ctx contractapi.TransactionContextInterface) (map[uint64]PhaseUpdateInfo, error) {
	var phaseUpdateTimeline map[uint64]PhaseUpdateInfo
	phaseUpdateTimelineAsBytes, err := ctx.GetStub().GetState(PHASE_UPDATE_TIMELINE)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(phaseUpdateTimelineAsBytes, &phaseUpdateTimeline)
	return phaseUpdateTimeline, nil
}

func updateTotalStakingAddress(ctx contractapi.TransactionContextInterface, number int64) (*PhaseConfig, error) {
	phaseConfigAsBytes, err := ctx.GetStub().GetState("phaseConfig")
	if err != nil {
		return nil, err
	}
	if phaseConfigAsBytes == nil {
		return nil, fmt.Errorf("initialize chaincode first")
	}

	var phaseConfig PhaseConfig
	_ = json.Unmarshal(phaseConfigAsBytes, &phaseConfig)
	bigTotalStakingAddr, _ := new(big.Int).SetString(phaseConfig.TotalStakingAddr, 10)
	bigTotalStakingAddr = bigTotalStakingAddr.Add(bigTotalStakingAddr, new(big.Int).SetInt64(number))
	phaseConfig.TotalStakingAddr = bigTotalStakingAddr.String()

	phaseConfigAsBytes, _ = json.Marshal(phaseConfig)
	err = ctx.GetStub().PutState("phaseConfig", phaseConfigAsBytes)
	if err != nil {
		return nil, err
	}
	return &phaseConfig, nil
}

func CheckCredentials(ctx contractapi.TransactionContextInterface, name string, value string) error {
	attrValue, exists, err := ctx.GetClientIdentity().GetAttributeValue(name)
	if err != nil {
		return fmt.Errorf("error checking the defaultCreds %s", err.Error())
	}
	if attrValue != value || !exists {
		return fmt.Errorf("error invalid credentials")
	}
	return nil
}

// generateError common function to generate Errors
func generateError(statusCode uint64, errorCode string, message string) error {
	logger.Error("an Error with Errorcode", errorCode)
	return fmt.Errorf("%d$%s$%s", statusCode, errorCode, message)
}
