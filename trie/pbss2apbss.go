package trie

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/triedb"
	"github.com/ethereum/go-ethereum/trie/triedb/aggpathdb"
)

const (
	printLogPerIterateNumber = 10000000
	printLogPerElapsedSpan   = 10 * time.Second
)

type convertBatcher struct {
	reader                ethdb.KeyValueReader
	batchWriter           ethdb.Batch
	trieNodes             map[common.Hash]map[string]map[string][]byte // dirty trie node is use to update/delete.
	convertBatchThreshold uint64
	currentConvertNumber  uint64
}

func newConvertBatcher(db ethdb.Database, batchThreshold uint64) *convertBatcher {
	return &convertBatcher{
		reader:                db,
		batchWriter:           db.NewBatch(),
		trieNodes:             make(map[common.Hash]map[string]map[string][]byte),
		convertBatchThreshold: batchThreshold,
		currentConvertNumber:  0,
	}
}

func (c *convertBatcher) convert(owner common.Hash, pathKey []byte, blob []byte) error {
	var (
		ok         = true
		aggPathKey = aggpathdb.ToAggPath(pathKey)
	)

	_, ok = c.trieNodes[owner]
	if !ok {
		c.trieNodes[owner] = make(map[string]map[string][]byte)
	}
	_, ok = c.trieNodes[owner][string(aggPathKey)]
	if !ok {
		c.trieNodes[owner][string(aggPathKey)] = make(map[string][]byte)
	}
	_, ok = c.trieNodes[owner][string(aggPathKey)][string(pathKey)]
	if !ok {
		c.trieNodes[owner][string(aggPathKey)][string(pathKey)] = blob
	} else {
		log.Crit("Failed to convert due to repeated path key",
			"owner", owner.String(),
			"agg_path_key", common.Bytes2Hex(aggPathKey),
			"path_key", common.Bytes2Hex(pathKey))
	}

	c.currentConvertNumber++
	log.Debug("Convert trie node to agg node",
		"owner", owner.String(),
		"path_key", common.Bytes2Hex(pathKey),
		"agg_path_key", common.Bytes2Hex(aggPathKey),
		"node", NodeString(common.Hash{}.Bytes(), blob),
		"current_convert_number", c.currentConvertNumber)
	return c.writeToDB(false)
}

func (c *convertBatcher) writeToDB(force bool) error {
	if c.currentConvertNumber >= c.convertBatchThreshold || force {
		var (
			aggNodes = make(map[common.Hash]map[string]*aggpathdb.AggNode) // dirty diff, batch update
			ok       = true
		)

		for owner, ownerSubset := range c.trieNodes {
			_, ok = aggNodes[owner]
			if !ok {
				aggNodes[owner] = make(map[string]*aggpathdb.AggNode)
			}
			for aggPathKey, aggSubset := range ownerSubset {
				_, ok = aggNodes[owner][aggPathKey]
				if !ok {
					aggNode, err := aggpathdb.LoadAggNodeFromDatabase(c.reader, owner, []byte(aggPathKey))
					if err != nil {
						log.Error("Failed to load agg node from db",
							"owner", owner.String(),
							"agg_path_key", common.Bytes2Hex([]byte(aggPathKey)))
						return err
					}
					if aggNode == nil {
						aggNode = aggpathdb.NewAggNode()
					}
					aggNodes[owner][aggPathKey] = aggNode
				}
				for pathKey, blob := range aggSubset {
					aggNodes[owner][aggPathKey].Update([]byte(pathKey), blob)
					triedb.DeleteTrieNode(c.batchWriter, nil, owner, []byte(pathKey), common.Hash{}, rawdb.PathScheme)
					log.Debug("Delete trie node",
						"owner", owner.String(),
						"path_key", common.Bytes2Hex([]byte(pathKey)))

				}
			}
		}
		for owner, subset := range aggNodes {
			for aggPathKey, aggNode := range subset {
				aggpathdb.WriteAggNode(c.batchWriter, owner, []byte(aggPathKey), aggNode.EncodeToBytes())
				log.Debug("Overwrite agg node",
					"owner", owner.String(),
					"agg_path_key", common.Bytes2Hex([]byte(aggPathKey)))
			}
		}
		if err := c.batchWriter.Write(); err != nil {
			log.Error("Failed to db batch write", "error", err)
			return err
		}

		c.reset()
	}

	return nil
}

func (c *convertBatcher) reset() {
	c.batchWriter.Reset()
	c.trieNodes = make(map[common.Hash]map[string]map[string][]byte)
	c.currentConvertNumber = 0
	return
}

// Pbss2Apbss is a convert tool for converting path-base db to agg-path-base db.
type Pbss2Apbss struct {
	iterator ethdb.Iterator
	batcher  *convertBatcher
}

// NewPbss2Apbss return a Pbss2Apbss obj.
func NewPbss2Apbss(db ethdb.Database, batchNum uint64) *Pbss2Apbss {
	return &Pbss2Apbss{
		iterator: db.NewIterator(nil, nil),
		batcher:  newConvertBatcher(db, batchNum),
	}
}

// Run start to convert.
func (p2a *Pbss2Apbss) Run() error {
	var (
		err             error
		iterNumber      uint64
		accountNumber   uint64
		storageNumber   uint64
		startTimestamp  = time.Now()
		loggedTimestamp = time.Now()
	)

	defer p2a.iterator.Release()
	for p2a.iterator.Next() {
		if err = p2a.iterator.Error(); err != nil {
			log.Error("Failed to iterate db", "error", err)
			return err
		}

		var (
			key     = p2a.iterator.Key()
			value   = p2a.iterator.Value()
			ok      bool
			pathKey []byte
			owner   common.Hash
		)
		switch {
		case rawdb.IsAccountTrieNode(key):
			ok, pathKey = rawdb.ResolveAccountTrieNodeKey(key)
			if !ok {
				log.Crit("Failed to resolve account trie node key", "key", common.Bytes2Hex(key))
			}
			if err = p2a.batcher.convert(common.Hash{}, pathKey, common.CopyBytes(value)); err != nil {
				log.Error("Failed to convert account trie node to agg node", "path_key", common.Bytes2Hex(pathKey))
				return err
			}
			accountNumber++
		case rawdb.IsStorageTrieNode(key):
			ok, owner, pathKey = rawdb.ResolveStorageTrieNode(key)
			if !ok {
				log.Crit("Failed to resolve storage trie node key", "key", common.Bytes2Hex(key))
			}
			if err = p2a.batcher.convert(owner, pathKey, common.CopyBytes(value)); err != nil {
				log.Error("Failed to convert storage trie node to agg node",
					"owner", owner.String(),
					"path_key", common.Bytes2Hex(pathKey))
				return err
			}
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

	err = p2a.batcher.writeToDB(true)
	log.Info("Complete convert pbss to apbss",
		"iter_number", iterNumber,
		"account_number", accountNumber,
		"storage_number", storageNumber,
		"elapsed_time", common.PrettyDuration(time.Since(startTimestamp)),
		"error", err)
	return err
}
