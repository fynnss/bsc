package trie

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/triedb/aggpathdb"
)

const (
	printLogPerIterateNumber = 10000000
	printLogPerElapsedSpan   = 10 * time.Second
)

type Pbss2Apbss struct {
	db ethdb.Database
}

func NewPbss2Apbss(db ethdb.Database) (*Pbss2Apbss, error) {
	return &Pbss2Apbss{
		db: db,
	}, nil
}

func (p2a *Pbss2Apbss) Run() error {
	var (
		err             error
		batch           ethdb.Batch
		iter            ethdb.Iterator
		iterNumber      uint64
		accountNumber   uint64
		storageNumber   uint64
		startTimestamp  = time.Now()
		loggedTimestamp = time.Now()
	)

	batch = p2a.db.NewBatch()
	iter = p2a.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		var (
			key        []byte
			value      []byte
			ok         bool
			pathKey    []byte
			aggPathKey []byte
			aggNode    *aggpathdb.AggNode
		)

		if err = iter.Error(); err != nil {
			return err
		}

		key = iter.Key()
		value = iter.Value()
		switch {
		case rawdb.IsAccountTrieNode(key):
			ok, pathKey = rawdb.ResolveAccountTrieNodeKey(key)
			if !ok {
				log.Warn("Failed to resolve account trie node key", "key", common.Bytes2Hex(key))
				continue
			}
			aggPathKey = aggpathdb.ToAggPath(pathKey)
			aggNode, err = aggpathdb.LoadAggNodeFromDatabase(p2a.db, common.Hash{}, aggPathKey)
			if err != nil {
				return err
			}
			if aggNode == nil {
				aggNode = &aggpathdb.AggNode{}
			}
			aggNode.Update(pathKey, value)
			rawdb.WriteAccountTrieAggNode(batch, aggPathKey, aggNode.EncodeToBytes())
			rawdb.DeleteAccountTrieNode(batch, pathKey)
			err = batch.Write()
			if err != nil {
				return err
			}
			batch.Reset()
			log.Debug("Convert account node to agg node",
				"path_key", common.Bytes2Hex(pathKey),
				"agg_path_key", common.Bytes2Hex(aggPathKey))
			accountNumber++
		case rawdb.IsStorageTrieNode(key):
			var accountHash common.Hash
			ok, accountHash, pathKey = rawdb.ResolveStorageTrieNode(key)
			if !ok {
				log.Warn("Failed to resolve storage trie node key", "key", common.Bytes2Hex(key))
				continue
			}
			aggPathKey = aggpathdb.ToAggPath(pathKey)
			aggNode, err = aggpathdb.LoadAggNodeFromDatabase(p2a.db, accountHash, aggPathKey)
			if err != nil {
				return err
			}
			if aggNode == nil {
				aggNode = &aggpathdb.AggNode{}
			}
			aggNode.Update(pathKey, value)
			rawdb.WriteStorageTrieAggNode(batch, accountHash, aggPathKey, aggNode.EncodeToBytes())
			rawdb.DeleteStorageTrieNode(batch, accountHash, pathKey)
			err = batch.Write()
			if err != nil {
				return err
			}
			batch.Reset()
			log.Debug("Convert storage node to agg node",
				"path_key", common.Bytes2Hex(pathKey),
				"agg_path_key", common.Bytes2Hex(aggPathKey),
				"account_hash", accountHash.String())
			storageNumber++
		default:
		}

		iterNumber++
		if (iterNumber%printLogPerIterateNumber) == 0 || time.Since(loggedTimestamp) > printLogPerElapsedSpan {
			log.Info("Convert pbss to apbss progress",
				"iter_number", iterNumber,
				"account_number", accountNumber,
				"storage_number", storageNumber,
				"current_key", common.Bytes2Hex(key),
				"elapsed_time", common.PrettyDuration(time.Since(startTimestamp)))
			loggedTimestamp = time.Now()
		}
	}

	// p2a.db.Compact()

	log.Info("Complete convert pbss to apbss",
		"iter_number", iterNumber,
		"account_number", accountNumber,
		"storage_number", storageNumber,
		"elapsed_time", common.PrettyDuration(time.Since(startTimestamp)))
	return err
}
