package main

import (
	"time"
)

// User user on busy blockchain
type User struct {
	DocType       string         `json:"docType"`
	UserID        string         `json:"userId"`
	DefaultWallet string         `json:"defaultWallet"`
	MessageCoins  map[string]int `json:"messageCoins"`
}

type Wallet struct {
	DocType   string `json:"docType"`
	UserID    string `json:"userId"`
	Address   string `json:"address"`
	Balance   string `json:"balance"`
	CreatedAt uint64 `json:"createdAt"`
}

// UTXO unspent transaction output
type UTXO struct {
	DocType string `json:"docType"`
	Address string `json:"address"`
	Amount  string `json:"amount"`
	Token   string `json:"token"`
}

type Token struct {
	DocType      string        `json:"docType"`
	ID           uint64        `json:"id"`
	TokenName    string        `json:"tokenName"`
	TokenSymbol  string        `json:"tokenSymbol"`
	Admin        string        `json:"admin"`
	TotalSupply  string        `json:"totalSupply"`
	Decimals     uint64        `json:"decimals"`
	MetaData     TokenMetaData `json:"tokenMetaData"`
	TokenAddress string        `json:"tokenAddress"`
}

// Response response will be returned in this format
type Response struct {
	TxID    string      `json:"txId"`
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// LockedTokens locked tokens
type LockedTokens struct {
	DocType        string `json:"docType"`
	TotalAmount    string `json:"totalAmount"`
	ReleasedAmount string `json:"releasedAmount"`
	StartedAt      uint64 `json:"startedAt"`
	ReleaseAt      uint64 `json:"releaseAt"`
}

// Pool represents the data of overall Governance Voting
type Pool struct {
	DocType          string    `json:"docType"`
	PoolID           string    `json:"poolId"`
	CreatedBy        string    `json:"createdBy"`
	CreatedAt        time.Time `json:"createdAt"`
	VotingStartAt    time.Time `json:"votingStartAt"`
	VotingEndAt      time.Time `json:"votingEndAt"`
	VotingAddressYes string    `json:"votingAddressYes"`
	VotingAddressNo  string    `json:"votingAddressNo"`
	VotingPowerYes   string    `json:"votingPowerYes"`
	VotingPowerNo    string    `json:"votingPowerNo"`
	TokenType        string    `json:"tokenType"`
	PoolName         string    `json:"poolName"`
	PoolDescription  string    `json:"poolDescription"`
	PoolFee          string    `json:"poolFee"`
}

// Vote represents the tokens given by individual vote to the pool
type Vote struct {
	DocType     string    `json:"docType"`
	VoteTime    time.Time `json:"voteTime"`
	VoteAddress string    `json:"voteAddress"`
	Tokens      string    `json:"tokens"`
	VoteType    string    `json:"votetype"`
}

// MessageConfig to set intial configuration for BusyCoins
type MessageConfig struct {
	// BusyCoins to deduct
	BigBusyCoins    string        `json:"bigBusyCoins"`
	MessageInterval time.Duration `json:"messageInterval"`
	BusyCoin        int           `json:"busyCoin"`
}

// PhaseConfig to store phase config
type PhaseConfig struct {
	CurrentPhase          uint64 `json:"currentPhase"`
	TotalStakingAddr      string `json:"totalStakingAddr"`
	NextStakingAddrTarget string `json:"nextStakingAddrTarget"`
	CurrentStakingLimit   string `json:"currentStakingLimit"`
}

// VotingConfig to set Configuration for Voting
type VotingConfig struct {
	MinimumCoins    string        `json:"minimumCoins"`
	PoolFee         string        `json:"poolFee"`
	VotingPeriod    time.Duration `json:"votingPeriod"`
	VotingStartTime time.Duration `json:"votingStartTime"`
}

// StakingInfo store info regarding at which time staking was done
type StakingInfo struct {
	DocType             string `json:"docType"`
	StakingAddress      string `json:"stakingAddr"`
	InitialStakingLimit string `json:"initialStakingLimit"`
	StakedCoins         string `json:"stakedCoins"`
	TimeStamp           uint64 `json:"timestamp"`
	Phase               uint64 `json:"phase"`
	// TotalReward It will be zero in Blockchain state but while showign staking info we update and return in response
	TotalReward          string `json:"totalReward"`
	Claimed              string `json:"claimed"`
	DefaultWalletAddress string `json:"defaultWalletAddress"`
	Unstaked             bool   `json:"unstaked"`
}

type PhaseUpdateInfo struct {
	UpdatedAt    uint64 `json:"updatedAt"`
	StakingLimit string `json:"stakingLimit"`
}
