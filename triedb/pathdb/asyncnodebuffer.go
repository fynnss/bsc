package pathdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/trie/triestate"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var _ trienodebuffer = &asyncnodebuffer{}

// asyncnodebuffer implement trienodebuffer interface, and async the nodecache
// to disk.
type asyncnodebuffer struct {
	mux          sync.RWMutex
	current      *nodecache
	background   *nodecache
	isFlushing   atomic.Bool
	stopFlushing atomic.Bool
}

func (a *asyncnodebuffer) account(hash common.Hash) ([]byte, bool) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	if node, exist := a.current.account(hash); exist {
		return node, exist
	}
	return a.background.account(hash)
}

func (a *asyncnodebuffer) storage(accountHash, storageHash common.Hash) ([]byte, bool) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	if node, exist := a.current.storage(accountHash, storageHash); exist {
		return node, exist
	}
	return a.background.storage(accountHash, storageHash)
}

// newAsyncNodeBuffer initializes the async node buffer with the provided nodes.
func newAsyncNodeBuffer(limit int,
	nodes map[common.Hash]map[string]*trienode.Node,
	latestAccounts map[common.Hash][]byte,
	latestStorages map[common.Hash]map[common.Hash][]byte,
	destructSet map[common.Hash]struct{},
	layers uint64) *asyncnodebuffer {
	if nodes == nil {
		nodes = make(map[common.Hash]map[string]*trienode.Node)
	}
	var size uint64
	for _, subset := range nodes {
		for path, n := range subset {
			size += uint64(len(n.Blob) + len(path))
		}
	}

	return &asyncnodebuffer{
		current:    newNodeCache(uint64(limit), size, nodes, latestAccounts, latestStorages, destructSet, layers),
		background: newNodeCache(uint64(limit), 0, make(map[common.Hash]map[string]*trienode.Node), nil, nil, nil, 0),
	}
}

// node retrieves the trie node with given node info.
func (a *asyncnodebuffer) node(owner common.Hash, path []byte, hash common.Hash) (*trienode.Node, error) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	node, err := a.current.node(owner, path, hash)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return a.background.node(owner, path, hash)
	}
	return node, nil
}

// commit merges the dirty nodes into the nodebuffer. This operation won't take
// the ownership of the nodes map which belongs to the bottom-most diff layer.
// It will just hold the node references from the given map which are safe to
// copy.
func (a *asyncnodebuffer) commit(nodes map[common.Hash]map[string]*trienode.Node, set *triestate.Set) trienodebuffer {
	a.mux.Lock()
	defer a.mux.Unlock()

	err := a.current.commit(nodes, set)
	if err != nil {
		log.Crit("[BUG] Failed to commit nodes to asyncnodebuffer", "error", err)
	}
	return a
}

// revert is the reverse operation of commit. It also merges the provided nodes
// into the nodebuffer, the difference is that the provided node set should
// revert the changes made by the last state transition.
func (a *asyncnodebuffer) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) error {
	a.mux.Lock()
	defer a.mux.Unlock()

	var err error
	a.current, err = a.current.merge(a.background)
	if err != nil {
		log.Crit("[BUG] Failed to merge node cache under revert async node buffer", "error", err)
	}
	a.background.reset()
	return a.current.revert(db, nodes)
}

// setSize is unsupported in asyncnodebuffer, due to the double buffer, blocking will occur.
func (a *asyncnodebuffer) setSize(size int, db ethdb.KeyValueStore, clean *cleanCache, id uint64) error {
	return errors.New("not supported")
}

// reset cleans up the disk cache.
func (a *asyncnodebuffer) reset() {
	a.mux.Lock()
	defer a.mux.Unlock()

	a.current.reset()
	a.background.reset()
}

// empty returns an indicator if nodebuffer contains any state transition inside.
func (a *asyncnodebuffer) empty() bool {
	a.mux.RLock()
	defer a.mux.RUnlock()

	return a.current.empty() && a.background.empty()
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (a *asyncnodebuffer) flush(db ethdb.KeyValueStore, clean *cleanCache, id uint64, force bool) error {
	a.mux.Lock()
	defer a.mux.Unlock()

	if a.stopFlushing.Load() {
		return nil
	}

	if force {
		for {
			if atomic.LoadUint64(&a.background.immutable) == 1 {
				time.Sleep(time.Duration(DefaultBackgroundFlushInterval) * time.Second)
				log.Info("Waiting background memory table flushed into disk for forcing flush node buffer")
				continue
			}
			atomic.StoreUint64(&a.current.immutable, 1)
			return a.current.flush(db, clean, id)
		}
	}

	if a.current.size < a.current.limit {
		return nil
	}

	// background flush doing
	if atomic.LoadUint64(&a.background.immutable) == 1 {
		return nil
	}

	atomic.StoreUint64(&a.current.immutable, 1)
	a.current, a.background = a.background, a.current

	a.isFlushing.Store(true)
	go func(persistID uint64) {
		defer a.isFlushing.Store(false)
		for {
			err := a.background.flush(db, clean, persistID)
			if err == nil {
				log.Debug("Succeed to flush background nodecache to disk", "state_id", persistID)
				return
			}
			log.Error("Failed to flush background nodecache to disk", "state_id", persistID, "error", err)
		}
	}(id)
	return nil
}

func (a *asyncnodebuffer) waitAndStopFlushing() {
	a.stopFlushing.Store(true)
	for a.isFlushing.Load() {
		time.Sleep(time.Second)
		log.Warn("Waiting background memory table flushed into disk")
	}
}

func (a *asyncnodebuffer) getAllNodes() map[common.Hash]map[string]*trienode.Node {
	a.mux.Lock()
	defer a.mux.Unlock()

	cached, err := a.current.merge(a.background)
	if err != nil {
		log.Crit("[BUG] Failed to merge node cache under revert async node buffer", "error", err)
	}
	return cached.nodes
}

func (a *asyncnodebuffer) getLatestStates() *triestate.Set {
	a.mux.Lock()
	defer a.mux.Unlock()

	res := triestate.New(nil, nil, nil, a.background.LatestAccounts, a.background.LatestStorages, a.background.DestructSet)

	for accHash, _ := range a.current.DestructSet {
		delete(res.LatestAccounts, accHash)
		delete(res.LatestStorages, accHash)
		res.DestructSet[accHash] = struct{}{}
	}

	for accHash, v := range a.current.LatestAccounts {
		res.LatestAccounts[accHash] = v
	}
	for accHash, storages := range a.current.LatestStorages {
		if _, ok := res.LatestStorages[accHash]; !ok {
			res.LatestStorages[accHash] = storages
		} else {
			for h, v := range storages {
				res.LatestStorages[accHash][h] = v
			}
		}
	}
	return res
}

func (a *asyncnodebuffer) getLayers() uint64 {
	a.mux.RLock()
	defer a.mux.RUnlock()

	return a.current.layers + a.background.layers
}

func (a *asyncnodebuffer) getSize() (uint64, uint64) {
	a.mux.RLock()
	defer a.mux.RUnlock()

	return a.current.size, a.background.size
}

type nodecache struct {
	layers uint64                                    // The number of diff layers aggregated inside
	size   uint64                                    // The size of aggregated writes
	limit  uint64                                    // The maximum memory allowance in bytes
	nodes  map[common.Hash]map[string]*trienode.Node // The dirty node set, mapped by owner and path

	// latest account and storage
	LatestAccounts map[common.Hash][]byte
	LatestStorages map[common.Hash]map[common.Hash][]byte
	DestructSet    map[common.Hash]struct{}

	immutable uint64 // The flag equal 1, flush nodes to disk background
}

func newNodeCache(limit, size uint64,
	nodes map[common.Hash]map[string]*trienode.Node,
	latestAccounts map[common.Hash][]byte,
	latestStorages map[common.Hash]map[common.Hash][]byte,
	destructSet map[common.Hash]struct{},
	layers uint64) *nodecache {
	if latestStorages == nil {
		latestStorages = make(map[common.Hash]map[common.Hash][]byte)
	}
	if latestAccounts == nil {
		latestAccounts = make(map[common.Hash][]byte)
	}
	if destructSet == nil {
		destructSet = make(map[common.Hash]struct{})
	}
	return &nodecache{
		layers: layers,
		size:   size,
		limit:  limit,
		nodes:  nodes,

		LatestAccounts: latestAccounts,
		LatestStorages: latestStorages, DestructSet: destructSet,
		immutable: 0,
	}
}

func (nc *nodecache) account(hash common.Hash) ([]byte, bool) {
	if data, ok := nc.LatestAccounts[hash]; ok {
		return data, true
	}

	if _, ok := nc.DestructSet[hash]; ok {
		return nil, true
	}
	return nil, false
}

func (nc *nodecache) storage(accountHash, storageHash common.Hash) ([]byte, bool) {
	if storage, ok := nc.LatestStorages[accountHash]; ok {
		if data, ok := storage[storageHash]; ok {
			return data, true
		}
	}

	if _, ok := nc.DestructSet[accountHash]; ok {
		return nil, true
	}
	return nil, false
}

func (nc *nodecache) node(owner common.Hash, path []byte, hash common.Hash) (*trienode.Node, error) {
	subset, ok := nc.nodes[owner]
	if !ok {
		return nil, nil
	}
	n, ok := subset[string(path)]
	if !ok {
		return nil, nil
	}
	if n.Hash != hash {
		dirtyFalseMeter.Mark(1)
		log.Error("Unexpected trie node in async node buffer", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
		return nil, newUnexpectedNodeError("dirty", hash, n.Hash, owner, path, n.Blob)
	}
	return n, nil
}

func (nc *nodecache) commit(nodes map[common.Hash]map[string]*trienode.Node, set *triestate.Set) error {
	if atomic.LoadUint64(&nc.immutable) == 1 {
		return errWriteImmutable
	}

	var (
		delta         int64
		overwrite     int64
		overwriteSize int64
	)
	// Delete the whole deleted account
	for h := range set.DestructSet {
		delete(nc.LatestStorages, h)
		delete(nc.LatestAccounts, h)
		delete(nc.nodes, h)
		nc.DestructSet[h] = struct{}{}
	}
	for h, acc := range set.LatestAccounts {
		nc.LatestAccounts[h] = acc
	}
	for h, storages := range set.LatestStorages {
		currents, ok := nc.LatestStorages[h]
		if !ok {
			currents = make(map[common.Hash][]byte)
			for k, v := range storages {
				currents[k] = v
			}
			nc.LatestStorages[h] = currents
			continue
		}
		for k, v := range storages {
			currents[k] = v
		}
		nc.LatestStorages[h] = currents
	}

	for owner, subset := range nodes {
		current, exist := nc.nodes[owner]
		if !exist {
			// Allocate a new map for the subset instead of claiming it directly
			// from the passed map to avoid potential concurrent map read/write.
			// The nodes belong to original diff layer are still accessible even
			// after merging, thus the ownership of nodes map should still belong
			// to original layer and any mutation on it should be prevented.
			current = make(map[string]*trienode.Node)
			for path, n := range subset {
				current[path] = n
				delta += int64(len(n.Blob) + len(path))
			}
			nc.nodes[owner] = current
			continue
		}
		for path, n := range subset {
			if orig, exist := current[path]; !exist {
				delta += int64(len(n.Blob) + len(path))
			} else {
				delta += int64(len(n.Blob) - len(orig.Blob))
				overwrite++
				overwriteSize += int64(len(orig.Blob) + len(path))
			}
			current[path] = n
		}
		nc.nodes[owner] = current
	}

	nc.updateSize(delta)
	nc.layers++
	gcNodesMeter.Mark(overwrite)
	gcBytesMeter.Mark(overwriteSize)
	return nil
}

func (nc *nodecache) updateSize(delta int64) {
	size := int64(nc.size) + delta
	if size >= 0 {
		nc.size = uint64(size)
		return
	}
	s := nc.size
	nc.size = 0
	log.Error("Invalid pathdb buffer size", "prev", common.StorageSize(s), "delta", common.StorageSize(delta))
}

func (nc *nodecache) reset() {
	atomic.StoreUint64(&nc.immutable, 0)
	nc.layers = 0
	nc.size = 0
	nc.nodes = make(map[common.Hash]map[string]*trienode.Node)
	nc.LatestAccounts = make(map[common.Hash][]byte)
	nc.LatestStorages = make(map[common.Hash]map[common.Hash][]byte)
	nc.DestructSet = make(map[common.Hash]struct{})
}

func (nc *nodecache) empty() bool {
	return nc.layers == 0
}

func (nc *nodecache) flush(db ethdb.KeyValueStore, clean *cleanCache, id uint64) error {
	if atomic.LoadUint64(&nc.immutable) != 1 {
		return errFlushMutable
	}

	// Ensure the target state id is aligned with the internal counter.
	head := rawdb.ReadPersistentStateID(db)
	if head+nc.layers != id {
		return fmt.Errorf("buffer layers (%d) cannot be applied on top of persisted state id (%d) to reach requested state id (%d)", nc.layers, head, id)
	}
	var (
		start = time.Now()
		batch = db.NewBatchWithSize(int(float64(nc.size) * DefaultBatchRedundancyRate))
	)
	// delete all kv for destructSet first to keep latest for disk nodes
	for h, _ := range nc.DestructSet {
		rawdb.DeleteStorageTrie(batch, h)
		clean.latestStates.Set(h.Bytes(), nil)
	}
	for h, acc := range nc.LatestAccounts {
		clean.latestStates.Set(h.Bytes(), acc)
	}
	// write all the nodes
	nodes := writeNodes(batch, nc.nodes, clean)
	rawdb.WritePersistentStateID(batch, id)

	// Flush all mutations in a single batch
	size := batch.ValueSize()
	if err := batch.Write(); err != nil {
		return err
	}

	commitBytesMeter.Mark(int64(size))
	commitNodesMeter.Mark(int64(nodes))
	commitTimeTimer.UpdateSince(start)
	log.Debug("Persisted pathdb nodes", "nodes", len(nc.nodes), "bytes", common.StorageSize(size), "elapsed", common.PrettyDuration(time.Since(start)))
	nc.reset()
	return nil
}

func (nc *nodecache) merge(nc1 *nodecache) (*nodecache, error) {
	if nc == nil && nc1 == nil {
		return nil, nil
	}
	if nc == nil || nc.empty() {
		res := copyNodeCache(nc1)
		atomic.StoreUint64(&res.immutable, 0)
		return res, nil
	}
	if nc1 == nil || nc1.empty() {
		res := copyNodeCache(nc)
		atomic.StoreUint64(&res.immutable, 0)
		return res, nil
	}
	if atomic.LoadUint64(&nc.immutable) == atomic.LoadUint64(&nc1.immutable) {
		return nil, errIncompatibleMerge
	}

	var (
		immutable *nodecache
		mutable   *nodecache
		res       = &nodecache{}
	)
	if atomic.LoadUint64(&nc.immutable) == 1 {
		immutable = nc
		mutable = nc1
	} else {
		immutable = nc1
		mutable = nc
	}
	res.size = immutable.size + mutable.size
	res.layers = immutable.layers + mutable.layers
	res.limit = immutable.limit
	res.nodes = make(map[common.Hash]map[string]*trienode.Node)
	for acc, subTree := range immutable.nodes {
		if _, ok := res.nodes[acc]; !ok {
			res.nodes[acc] = make(map[string]*trienode.Node)
		}
		for path, node := range subTree {
			res.nodes[acc][path] = node
		}
	}

	for acc, subTree := range mutable.nodes {
		if _, ok := res.nodes[acc]; !ok {
			res.nodes[acc] = make(map[string]*trienode.Node)
		}
		for path, node := range subTree {
			res.nodes[acc][path] = node
		}
	}
	return res, nil
}

func (nc *nodecache) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) error {
	if atomic.LoadUint64(&nc.immutable) == 1 {
		return errRevertImmutable
	}

	// Short circuit if no embedded state transition to revert.
	if nc.layers == 0 {
		return errStateUnrecoverable
	}
	nc.layers--

	// Reset the entire buffer if only a single transition left.
	if nc.layers == 0 {
		nc.reset()
		return nil
	}
	var delta int64
	for owner, subset := range nodes {
		current, ok := nc.nodes[owner]
		if !ok {
			panic(fmt.Sprintf("non-existent subset (%x)", owner))
		}
		for path, n := range subset {
			orig, ok := current[path]
			if !ok {
				// There is a special case in MPT that one child is removed from
				// a fullNode which only has two children, and then a new child
				// with different position is immediately inserted into the fullNode.
				// In this case, the clean child of the fullNode will also be
				// marked as dirty because of node collapse and expansion.
				//
				// In case of database rollback, don't panic if this "clean"
				// node occurs which is not present in buffer.
				var nhash common.Hash
				if owner == (common.Hash{}) {
					_, nhash = rawdb.ReadAccountTrieNode(db, []byte(path))
				} else {
					_, nhash = rawdb.ReadStorageTrieNode(db, owner, []byte(path))
				}
				// Ignore the clean node in the case described above.
				if nhash == n.Hash {
					continue
				}
				panic(fmt.Sprintf("non-existent node (%x %v) blob: %v", owner, path, crypto.Keccak256Hash(n.Blob).Hex()))
			}
			current[path] = n
			delta += int64(len(n.Blob)) - int64(len(orig.Blob))
		}
	}
	nc.updateSize(delta)
	return nil
}

func copyNodeCache(n *nodecache) *nodecache {
	if n == nil {
		return nil
	}
	nc := &nodecache{
		layers:    n.layers,
		size:      n.size,
		limit:     n.limit,
		immutable: atomic.LoadUint64(&n.immutable),
		nodes:     make(map[common.Hash]map[string]*trienode.Node),
	}
	for acc, subTree := range n.nodes {
		if _, ok := nc.nodes[acc]; !ok {
			nc.nodes[acc] = make(map[string]*trienode.Node)
		}
		for path, node := range subTree {
			nc.nodes[acc][path] = node
		}
	}
	return nc
}
