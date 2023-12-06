package aggpathdb

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/sync/singleflight"
)

var g singleflight.Group

type aggNodeCache struct {
	cleans *fastcache.Cache
	db     *Database // Agg-Path-based trie database
}

func newAggNodeCache(db *Database, cleans *fastcache.Cache, cacheSize int) *aggNodeCache {
	if cleans == nil {
		cleans = fastcache.New(cacheSize)
	}

	log.Info("Allocated node cache", "size", cacheSize)
	return &aggNodeCache{
		cleans: cleans,
		db:     db,
	}
}

func (c *aggNodeCache) node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	aggPath := ToAggPath(path)
	key := cacheKey(owner, aggPath)

	if c.cleans != nil {
		if blob := c.cleans.Get(nil, key); len(blob) > 0 {
			aggNode, err := DecodeAggNode(blob)
			if err != nil {
				return nil, fmt.Errorf("decode node failed from clean cache. error: %v", err)
			}

			n := aggNode.Node(path)
			if n == nil {
				// not found
				return []byte{}, nil
			}

			if n.Hash == hash {
				cleanHitMeter.Mark(1)
				cleanReadMeter.Mark(int64(len(blob)))
				return n.Blob, nil
			}
			cleanFalseMeter.Mark(1)
			log.Error("Unexpected trie node in clean cache", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
		}
		cleanMissMeter.Mark(1)
	}

	// Try to retrieve the trie node from the disk.
	v, err, _ := g.Do(string(key), func() (interface{}, error) {
		start := time.Now()
		var originBlob []byte
		// try to get node from the database
		if owner == (common.Hash{}) {
			originBlob = rawdb.ReadAccountTrieAggNode(c.db.diskdb, aggPath)
		} else {
			originBlob = rawdb.ReadStorageTrieAggNode(c.db.diskdb, owner, aggPath)
		}
		diskLayerRawNodeTimer.UpdateSince(start)
		if originBlob == nil {
			return []byte{}, nil
		}
		aggNode, err := DecodeAggNode(originBlob)
		if err != nil {
			return nil, fmt.Errorf("decode node failed from diskdb. error: %v", err)
		}
		n := aggNode.Node(path)
		if n == nil {
			// not found
			return []byte{}, nil
		}

		if n.Hash != hash {
			diskFalseMeter.Mark(1)
			log.Error("Unexpected trie node in disk", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
			return nil, newUnexpectedNodeError("disk", hash, n.Hash, owner, path, n.Blob)
		}
		if c.cleans != nil {
			c.cleans.Set(key, originBlob)
			cleanWriteMeter.Mark(int64(len(originBlob)))
		}
		return n.Blob, nil
	})
	//if shared { // debug
	//	log.Info("single flight", "owner", owner, "path", path)
	//}
	if err != nil {
		return []byte{}, err
	}
	nBlob := v.([]byte)
	if nBlob == nil {
		// not found
		return []byte{}, nil
	}
	return nBlob, nil
}

func (c *aggNodeCache) aggNode(owner common.Hash, aggPath []byte) (*AggNode, error) {
	var blob []byte
	cKey := cacheKey(owner, aggPath)
	if c.cleans != nil {
		cacheHit := false
		blob, cacheHit = c.cleans.HasGet(nil, cKey)
		if cacheHit {
			return DecodeAggNode(blob)
		}
	}

	// cache miss
	if owner == (common.Hash{}) {
		blob = rawdb.ReadAccountTrieAggNode(c.db.diskdb, aggPath)
	} else {
		blob = rawdb.ReadStorageTrieAggNode(c.db.diskdb, owner, aggPath)
	}
	if blob == nil {
		return nil, nil
	}

	return DecodeAggNode(blob)
}

func (c *aggNodeCache) Reset() {
	c.cleans.Reset()
}

func (c *aggNodeCache) Del(k []byte) {
	if c.cleans != nil {
		c.cleans.Del(k)
	}
}

func (c *aggNodeCache) Set(k, v []byte) {
	if c.cleans != nil {
		c.cleans.Set(k, v)
	}
}

func (c *aggNodeCache) HasGet(dst, k []byte) ([]byte, bool) {
	if c.cleans != nil {
		return c.cleans.HasGet(dst, k)
	}
	return nil, false
}
