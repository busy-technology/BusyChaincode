package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type LastMessage struct {
	MessageTime time.Time
	Sender      string
	Recipient   string
}

// BusyMessenger contract
type BusyMessenger struct {
	contractapi.Contract
}

// MessageInfo
type MessageStore struct {
	Sender    map[string]int
	Recipient map[string]int
}

// CreateUser creates new user on busy blockchain
func (bm *BusyMessenger) SendMessage(ctx contractapi.TransactionContextInterface, recipient string, token string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	err := CheckCredentials(ctx, DEFAULT_CREDS, "true")
	if err == nil {
		response.Message = "Credentials are not valid for SendMessage"
		logger.Error(response.Message)
		return response, generateError(403, "ATU001", response.Message)
	}

	err = CheckCredentials(ctx, "messageCreds", "true")
	if err != nil {
		response.Message = fmt.Sprintf("Error occurred while validating credentials: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME001", response.Message)
	}

	sender, _ := getCommonName(ctx)
	senderUserId := sender[2:]
	senderAsBytes, err := ctx.GetStub().GetState(senderUserId)
	if senderAsBytes == nil {
		response.Message = fmt.Sprintf("Sender with common name %s does not exists", sender)
		logger.Info(response.Message)
		return response, generateError(404, "SME002", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME003", response.Message)
	}

	senderDetails := User{}
	if err := json.Unmarshal(senderAsBytes, &senderDetails); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the sender details %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME004", response.Message)
	}

	recipientWalletAsBytes, err := ctx.GetStub().GetState(recipient)
	if recipientWalletAsBytes == nil {
		response.Message = fmt.Sprintf("Recipient with walletId  %s does not exist", recipient)
		logger.Info(response.Message)
		return response, generateError(404, "SME005", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME006", response.Message)
	}

	recipientWallet := Wallet{}
	if err := json.Unmarshal(recipientWalletAsBytes, &recipientWallet); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the recipient details %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME007", response.Message)
	}

	logger.Info("Recieved a message from", senderDetails.DefaultWallet, "to", recipient)

	// getting the default config for messaging functionality
	configAsBytes, err := ctx.GetStub().GetState("MessageConfig")
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME008", response.Message)
	}
	var config MessageConfig
	if err = json.Unmarshal(configAsBytes, &config); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME009", response.Message)
	}

	// getting the last Message(time, sender and reciever) State for a single user
	lastMessageAsBytes, err := ctx.GetStub().GetState(getLastMessageKey(senderUserId))
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting last message state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME010", response.Message)
	}
	if lastMessageAsBytes != nil {
		var lastMessage LastMessage
		_ = json.Unmarshal(lastMessageAsBytes, &lastMessage)
		if time.Since(lastMessage.MessageTime) < config.MessageInterval {
			response.Message = "Please wait for 1 seconds before sending the next message"
			logger.Error(response.Message)
			return response, generateError(400, "SME011", response.Message)
		}
	}
	//updating the last Message
	lastMessage := LastMessage{
		MessageTime: time.Now(),
		Sender:      senderDetails.DefaultWallet,
		Recipient:   recipient,
	}
	lastMessageAsBytes, _ = json.Marshal(lastMessage)
	err = ctx.GetStub().PutState(getLastMessageKey(senderUserId), lastMessageAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME012", response.Message)
	}

	if senderDetails.DefaultWallet == recipient {
		//response.Message = fmt.Sprintf("message cannot be sent to the same userId: %s", sender)
		response.Message = "You cannot send the message to yourself"
		logger.Info(response.Message)
		return response, generateError(412, "SME013", response.Message)
	}

	recipientAsBytes, err := ctx.GetStub().GetState(recipientWallet.UserID)
	if recipientAsBytes == nil {
		response.Message = fmt.Sprintf("Recipient %s does not exists", recipient)
		logger.Info(response.Message)
		return response, generateError(404, "SME014", response.Message)
	}
	if err != nil {
		response.Message = fmt.Sprintf("Error while fetching user from blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME015", response.Message)
	}

	recipientDetails := User{}
	if err := json.Unmarshal(recipientAsBytes, &recipientDetails); err != nil {
		response.Message = fmt.Sprintf("Error while retrieving the recipient details %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "SME016", response.Message)
	}

	val, ok := senderDetails.MessageCoins[recipientDetails.DefaultWallet]

	var messagestore MessageStore
	// using MessageStore
	if ok && val > 0 {
		logger.Info("Using the message store")
		if err := AddCoins(ctx, recipientDetails.DefaultWallet, config.BigBusyCoins, token); err != nil {
			response.Message = fmt.Sprintf("Error while adding coins to the recipient default wallet %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME017", response.Message)
		}
		if val == config.BusyCoin {
			// deleting the key from map
			delete(senderDetails.MessageCoins, recipientDetails.DefaultWallet)
		} else {
			senderDetails.MessageCoins[recipientDetails.DefaultWallet] = val - config.BusyCoin
		}
		senderDetails.MessageCoins["totalCoins"] -= config.BusyCoin
		senderAsBytes, err = json.Marshal(senderDetails)
		if err != nil {
			response.Message = fmt.Sprintf("Error while marshalling the sender details %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME018", response.Message)
		}
		err = ctx.GetStub().PutState(senderUserId, senderAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME019", response.Message)
		}
		messagestore.Sender = senderDetails.MessageCoins
		messagestore.Recipient = recipientDetails.MessageCoins
	} else {
		logger.Info("using default wallet")

		balance, _ := getBalanceHelper(ctx, senderDetails.DefaultWallet, token)
		amountInt, _ := new(big.Int).SetString(config.BigBusyCoins, 10)
		if balance.Cmp(amountInt) == -1 {
			//response.Message = fmt.Sprintf("User: %s does not have enough coins to Send Message", sender)
			response.Message = "You do not have enough coins to send a message"
			logger.Error(response.Message)
			return response, generateError(402, "SME020", response.Message)
		}

		if err := RemoveCoins(ctx, senderDetails.DefaultWallet, config.BigBusyCoins, token); err != nil {
			response.Message = fmt.Sprintf("Error while adding coins to the recipient default wallet %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME021", response.Message)
		}
		if val, ok := recipientDetails.MessageCoins[senderDetails.DefaultWallet]; ok {
			recipientDetails.MessageCoins[senderDetails.DefaultWallet] = val + config.BusyCoin
		} else {
			recipientDetails.MessageCoins[senderDetails.DefaultWallet] = config.BusyCoin
		}
		recipientDetails.MessageCoins["totalCoins"] += config.BusyCoin

		recipientAsBytes, err = json.Marshal(recipientDetails)
		if err != nil {
			response.Message = fmt.Sprintf("Error while marshalling the recipient details %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME022", response.Message)
		}
		err = ctx.GetStub().PutState(recipientWallet.UserID, recipientAsBytes)
		if err != nil {
			response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
			logger.Error(response.Message)
			return response, generateError(500, "SME023", response.Message)
		}
		messagestore.Sender = senderDetails.MessageCoins
		messagestore.Recipient = recipientDetails.MessageCoins
	}
	balanceData := BalanceEvent{
		UserAddresses: []UserAddress{
			{
				Address: senderDetails.DefaultWallet,
				Token:   BUSY_COIN_SYMBOL,
			},
			{
				Address: recipientDetails.DefaultWallet,
				Token:   BUSY_COIN_SYMBOL,
			},
		},
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
	response.Data = messagestore
	response.Message = "Message has been sent successfully"
	response.Success = true
	return response, nil
}

//function to update messaging fee
func (bm *BusyMessenger) UpdateMessagingFee(ctx contractapi.TransactionContextInterface, newFee string) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	//check whether admin or not

	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed update the messaging fee"
		logger.Error(response.Message)
		return response, generateError(403, "UMSF001", response.Message)
	}

	// getting the default config for messaging functionality
	configAsBytes, err := ctx.GetStub().GetState("MessageConfig")
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UMSF002", response.Message)
	}
	var config MessageConfig
	if err = json.Unmarshal(configAsBytes, &config); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UMSF003", response.Message)
	}

	//updating the messaging fee

	//validate the newFee using BigInt
	config.BigBusyCoins = newFee

	configAsBytes, _ = json.Marshal(config)
	err = ctx.GetStub().PutState("MessageConfig", configAsBytes)
	if err != nil {
		response.Message = fmt.Sprintf("Error while updating state in blockchain: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "UMSF004", response.Message)
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

	response.Data = config.BigBusyCoins
	response.Message = "Messaging fee has been updated successfully"
	response.Success = true
	return response, nil
}

func (bm *BusyMessenger) GetMessagingFee(ctx contractapi.TransactionContextInterface) (*Response, error) {
	response := &Response{
		TxID:    ctx.GetStub().GetTxID(),
		Success: false,
		Message: "",
		Data:    nil,
	}

	//check whether admin or not

	commonName, _ := getCommonName(ctx)
	if commonName != "busy_network" {
		response.Message = "You are not allowed update the messaging fee"
		logger.Error(response.Message)
		return response, generateError(403, "MSF001", response.Message)
	}

	// getting the default config for messaging functionality
	configAsBytes, err := ctx.GetStub().GetState("MessageConfig")
	if err != nil {
		response.Message = fmt.Sprintf("Error while getting config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MSF002", response.Message)
	}
	var config MessageConfig
	if err = json.Unmarshal(configAsBytes, &config); err != nil {
		response.Message = fmt.Sprintf("Error while unmarshalling the config state: %s", err.Error())
		logger.Error(response.Message)
		return response, generateError(500, "MSF003", response.Message)
	}

	//updating the messaging fee

	//validate the newFee using BigInt

	response.Data = config.BigBusyCoins
	response.Message = "Current messaging fee has been successfully fetched"
	response.Success = true
	return response, nil
}

// RemoveCoins is to move coins from default wallet to message store
func RemoveCoins(ctx contractapi.TransactionContextInterface, address string, coins string, token string) error {
	minusOne, _ := new(big.Int).SetString("-1", 10)
	bigTxFee, _ := new(big.Int).SetString(coins, 10)

	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  bigTxFee.Mul(bigTxFee, minusOne).String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err := ctx.GetStub().PutState(fmt.Sprintf("message~%s~%s~%s", ctx.GetStub().GetTxID(), address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}

// RemoveCoins is to move coins from default wallet to message store
func AddCoins(ctx contractapi.TransactionContextInterface, address string, coins string, token string) error {
	plusOne, _ := new(big.Int).SetString("1", 10)
	bigTxFee, _ := new(big.Int).SetString(coins, 10)

	utxo := UTXO{
		DocType: "utxo",
		Address: address,
		Amount:  bigTxFee.Mul(bigTxFee, plusOne).String(),
		Token:   BUSY_COIN_SYMBOL,
	}
	utxoAsBytes, _ := json.Marshal(utxo)
	err := ctx.GetStub().PutState(fmt.Sprintf("message~%s~%s~%s", ctx.GetStub().GetTxID(), address, BUSY_COIN_SYMBOL), utxoAsBytes)
	if err != nil {
		return err
	}
	return nil
}
