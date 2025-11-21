package core

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

const balMismatchDir = "bal_root_mismatch"

type balDump struct {
	Mode        string      `json:"mode"`
	BlockNumber uint64      `json:"blockNumber"`
	BlockHash   string      `json:"blockHash"`
	LocalRoot   string      `json:"localRoot"`
	RemoteRoot  string      `json:"remoteRoot"`
	Timestamp   string      `json:"timestamp"`
	Version     uint32      `json:"version"`
	SignData    string      `json:"signData,omitempty"`
	AccessList  interface{} `json:"accessList"`
	TxHashes    []string    `json:"txHashes"`
}

func dumpBALToFile(block *types.Block, mode string, localRoot, remoteRoot common.Hash) {
	al := block.AccessList()
	if al == nil || al.AccessList == nil {
		return
	}
	if err := os.MkdirAll(balMismatchDir, 0o755); err != nil {
		log.Error("Failed to create BAL dump directory", "dir", balMismatchDir, "err", err)
		return
	}
	entry := balDump{
		Mode:        mode,
		BlockNumber: block.NumberU64(),
		BlockHash:   block.Hash().Hex(),
		LocalRoot:   localRoot.Hex(),
		RemoteRoot:  remoteRoot.Hex(),
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Version:     al.Version,
		AccessList:  al.AccessList,
	}
	if len(al.SignData) > 0 {
		entry.SignData = hex.EncodeToString(al.SignData)
	}
	for _, tx := range block.Transactions() {
		entry.TxHashes = append(entry.TxHashes, tx.Hash().Hex())
	}
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		log.Error("Failed to marshal BAL dump", "err", err)
		return
	}
	filename := fmt.Sprintf("bal_mismatch_%s_%d_%s_%d.json", mode, block.NumberU64(), block.Hash().Hex()[2:], time.Now().UnixNano())
	fullpath := filepath.Join(balMismatchDir, filename)
	if err := os.WriteFile(fullpath, data, 0o644); err != nil {
		log.Error("Failed to write BAL dump", "file", fullpath, "err", err)
	}
}
