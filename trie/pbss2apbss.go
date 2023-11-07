package trie

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/triedb"
	"github.com/ethereum/go-ethereum/trie/triedb/aggpathdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

const (
	printLogPerIterateNumber = 10000000
	printLogPerElapsedSpan   = 10 * time.Second
	convertBatchThreshold    = 16
)

type convertBatcher struct {
	reader             ethdb.KeyValueReader
	batchWriter        ethdb.Batch
	aggNodes           map[common.Hash]map[string]*aggpathdb.AggNode // need to update
	trieNodes          map[common.Hash]map[string]struct{}           // need to delete
	batchConvertNumber uint64
}

func newConvertBatcher(db ethdb.Database) *convertBatcher {
	return &convertBatcher{
		reader:             db,
		batchWriter:        db.NewBatch(),
		aggNodes:           make(map[common.Hash]map[string]*aggpathdb.AggNode),
		trieNodes:          make(map[common.Hash]map[string]struct{}),
		batchConvertNumber: 0,
	}
}

func (c *convertBatcher) convert(owner common.Hash, path []byte, value []byte) error {
	var (
		ok         = true
		aggPathKey = aggpathdb.ToAggPath(path)
	)

	_, ok = c.aggNodes[owner]
	if !ok {
		c.aggNodes[owner] = make(map[string]*aggpathdb.AggNode)
	}
	_, ok = c.aggNodes[owner][string(aggPathKey)]
	if !ok {
		aggNode, err := aggpathdb.LoadAggNodeFromDatabase(c.reader, owner, aggPathKey)
		if err != nil {
			log.Error("Failed to load agg node from db",
				"owner", owner.String(),
				"path_key", common.Bytes2Hex(path),
				"agg_path_key", common.Bytes2Hex(aggPathKey))
			return err
		}
		if aggNode == nil {
			aggNode = &aggpathdb.AggNode{}
		}
		c.aggNodes[owner][string(aggPathKey)] = aggNode
	}
	c.aggNodes[owner][string(aggPathKey)].Update(path, trienode.New(common.Hash{}, value))
	_, ok = c.trieNodes[owner]
	if !ok {
		c.trieNodes[owner] = make(map[string]struct{})
	}
	c.trieNodes[owner][string(path)] = struct{}{}

	log.Debug("Convert trie node to agg node",
		"owner", owner.String(),
		"path_key", common.Bytes2Hex(path),
		"agg_path_key", common.Bytes2Hex(aggPathKey))
	c.batchConvertNumber++
	return c.writeToDB(false)
}

func (c *convertBatcher) writeToDB(force bool) error {
	if c.batchConvertNumber >= convertBatchThreshold || force {
		for owner, subset := range c.aggNodes {
			for aggPath, aggNode := range subset {
				aggpathdb.WriteAggNode(c.batchWriter, owner, []byte(aggPath), aggNode.EncodeToBytes())
			}
		}
		for owner, subset := range c.trieNodes {
			for trieNodePath, _ := range subset {
				triedb.DeleteTrieNode(c.batchWriter, nil, owner, []byte(trieNodePath), common.Hash{}, rawdb.PathScheme)
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
	c.aggNodes = make(map[common.Hash]map[string]*aggpathdb.AggNode)
	c.trieNodes = make(map[common.Hash]map[string]struct{})
	c.batchConvertNumber = 0
	return
}

// Pbss2Apbss is a convert tool for converting path-base db to agg-path-base db.
type Pbss2Apbss struct {
	iterator ethdb.Iterator
	batcher  *convertBatcher
}

// NewPbss2Apbss return a Pbss2Apbss obj.
func NewPbss2Apbss(db ethdb.Database) *Pbss2Apbss {
	return &Pbss2Apbss{
		iterator: db.NewIterator(nil, nil),
		batcher:  newConvertBatcher(db),
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
			if err = p2a.batcher.convert(common.Hash{}, pathKey, value); err != nil {
				log.Error("Failed to convert account trie node to agg node", "path_key", common.Bytes2Hex(pathKey))
				return err
			}
			accountNumber++
		case rawdb.IsStorageTrieNode(key):
			ok, owner, pathKey = rawdb.ResolveStorageTrieNode(key)
			if !ok {
				log.Crit("Failed to resolve storage trie node key", "key", common.Bytes2Hex(key))
			}
			if err = p2a.batcher.convert(owner, pathKey, value); err != nil {
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
