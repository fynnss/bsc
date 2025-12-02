package state

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/sync/errgroup"
)

// TODO: probably unnecessary to cache the resolved state object here as it will already be in the db cache?
// ^ experiment with the performance of keeping this as-is vs just using the db cache.
type prestateResolver struct {
	inProgress  map[common.Address]chan struct{}
	resolved    sync.Map
	originSlots sync.Map
	ctx         context.Context
	cancel      func()
	resolveWG   sync.WaitGroup // WaitGroup to track resolution completion
}

func SlotKey(addr common.Address, slot common.Hash) string {
	return fmt.Sprintf("%s_%s", addr.Hex(), slot.Hex())
}

func (p *prestateResolver) resolve(r Reader, addrs map[common.Address][]common.Hash, ants *ants.Pool) {
	p.inProgress = make(map[common.Address]chan struct{})
	p.ctx, p.cancel = context.WithCancel(context.Background())

	for addr := range addrs {
		p.inProgress[addr] = make(chan struct{})
	}

	// Track the number of accounts to resolve
	p.resolveWG.Add(len(addrs))

	for addr, slots := range addrs {
		resolveAddr := addr
		resloveSlots := slots
		ants.Submit(func() {
			defer p.resolveWG.Done()
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			acct, err := r.Account(resolveAddr)
			if err != nil {
				log.Error("Failed to get account", "address", resolveAddr, "error", err)
				// TODO: what do here?
			}
			if len(resloveSlots) > 0 {
				// Use a separate goroutine for storage slots to avoid blocking account resolution
				ants.Submit(func() {
					for _, slot := range resloveSlots {
						select {
						case <-p.ctx.Done():
							return
						default:
						}
						oriSlot, err := r.Storage(resolveAddr, slot)
						if err != nil {
							log.Error("Failed to get storage", "address", resolveAddr, "slot", slot, "error", err)
						}
						p.originSlots.Store(SlotKey(resolveAddr, slot), oriSlot)
					}
				})
			}

			p.resolved.Store(resolveAddr, acct)
			close(p.inProgress[resolveAddr])
		})
	}
}

// WaitForAccounts waits for all account resolutions to complete.
// This ensures that account data is ready before transaction execution starts.
func (p *prestateResolver) WaitForAccounts() {
	p.resolveWG.Wait()
}

func (p *prestateResolver) account(addr common.Address) *types.StateAccount {
	if _, ok := p.inProgress[addr]; !ok {
		return nil
	}

	<-p.inProgress[addr]
	res, exist := p.resolved.Load(addr)
	if !exist {
		return nil
	}
	return res.(*types.StateAccount)
}

func (r *BALReader) initObjFromDiff(db *StateDB, addr common.Address, a *types.StateAccount, diff *bal.AccountMutations) *stateObject {
	var acct *types.StateAccount
	if a == nil {
		acct = &types.StateAccount{
			Nonce:    0,
			Balance:  uint256.NewInt(0),
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash[:],
		}
	} else {
		acct = a.Copy()
	}
	if diff == nil {
		return newObject(db, addr, acct)
	}

	if diff.Nonce != nil {
		acct.Nonce = *diff.Nonce
	}
	if diff.Balance != nil {
		acct.Balance = new(uint256.Int).Set(diff.Balance)
	}
	obj := newObject(db, addr, acct)
	if diff.Code != nil {
		obj.setCode(crypto.Keccak256Hash(diff.Code), diff.Code)
	}
	if diff.StorageWrites != nil {
		for key, val := range diff.StorageWrites {
			obj.pendingStorage[key] = val
		}
	}
	if obj.empty() {
		return nil
	}
	return obj
}

func (r *BALReader) initMutatedObjFromDiff(db *StateDB, addr common.Address, a *types.StateAccount, diff *bal.AccountMutations) (*stateObject, error) {
	var acct *types.StateAccount
	if a == nil {
		acct = &types.StateAccount{
			Nonce:    0,
			Balance:  uint256.NewInt(0),
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash[:],
		}
	} else {
		acct = a.Copy()
	}
	obj := newObject(db, addr, acct)
	if diff == nil {
		return obj, nil
	}
	if diff.Nonce != nil {
		obj.setNonce(*diff.Nonce)
	}
	if diff.Balance != nil {
		obj.setBalance(new(uint256.Int).Set(diff.Balance))
	}
	if diff.Code != nil {
		obj.setCodeModified(crypto.Keccak256Hash(diff.Code), diff.Code)
	}
	if diff.StorageWrites != nil {
		for key, val := range diff.StorageWrites {
			origin, err := r.loadOriginSlot(addr, key)
			if err != nil {
				return nil, err
			}
			if origin == val {
				continue
			}
			obj.originStorage[key] = origin
			obj.pendingStorage[key] = val
			obj.uncommittedStorage[key] = origin
		}
	}
	return obj, nil
}

func (r *BALReader) loadOriginSlot(addr common.Address, slot common.Hash) (common.Hash, error) {
	if origin, ok := r.prestateReader.originSlots.Load(SlotKey(addr, slot)); ok {
		return origin.(common.Hash), nil
	}
	if r.reader == nil {
		return common.Hash{}, fmt.Errorf("missing origin slot for %s", SlotKey(addr, slot))
	}
	origin, err := r.reader.Storage(addr, slot)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to load storage %s: %w", SlotKey(addr, slot), err)
	}
	r.prestateReader.originSlots.Store(SlotKey(addr, slot), origin)
	return origin, nil
}

// BALReader provides methods for reading account state from a block access
// list.  State values returned from the Reader methods must not be modified.
type BALReader struct {
	block          *types.Block
	accesses       map[common.Address]*bal.AccountAccess
	reader         Reader
	prestateReader prestateResolver
	prefetchOnce   sync.Once
}

// NewBALReader constructs a new reader from an access list. db is expected to have been instantiated with a reader.
func NewBALReader(block *types.Block, db *StateDB, ants *ants.Pool) *BALReader {
	r := &BALReader{
		accesses: make(map[common.Address]*bal.AccountAccess),
		block:    block,
		reader:   db.reader,
	}
	for _, acctDiff := range *block.AccessList().AccessList {
		r.accesses[acctDiff.Address] = &acctDiff
	}
	accountsWithStorage := make(map[common.Address][]common.Hash)
	// 预取所有在 BAL 中出现的账户，不仅仅是 ModifiedAccounts
	for addr, access := range r.accesses {
		// 收集需要预取的 storage slots
		var slots []common.Hash
		// 预取 StorageChanges（写入的存储槽）
		for _, change := range access.StorageChanges {
			slots = append(slots, change.Slot)
		}
		// 预取 StorageReads（读取的存储槽），确保交易执行时数据已准备好
		slots = append(slots, access.StorageReads...)

		// 如果有存储槽需要预取，或者账户有修改，则添加到预取列表
		if len(slots) > 0 || len(access.NonceChanges) != 0 || len(access.CodeChanges) != 0 ||
			len(access.BalanceChanges) != 0 {
			accountsWithStorage[addr] = slots
		}
	}
	r.prestateReader.resolve(db.Reader(), accountsWithStorage, ants)
	return r
}

// ModifiedAccounts returns a list of all accounts with mutations in the access list
func (r *BALReader) ModifiedAccounts() (res []common.Address) {
	for addr, access := range r.accesses {
		if len(access.NonceChanges) != 0 || len(access.CodeChanges) != 0 || len(access.StorageChanges) != 0 || len(access.BalanceChanges) != 0 {
			res = append(res, addr)
		}
	}
	return res
}

// PrefetchExecutionData exposes trie prefetching for callers which want to warm
// the cache before transaction execution. The actual scheduling logic is shared
// with the root-verification path to avoid duplication.
func (r *BALReader) PrefetchExecutionData(db *StateDB) {
	r.prefetchOnce.Do(func() {
		// 预取所有在 BAL 中出现的账户，不仅仅是 ModifiedAccounts
		// 这包括只读账户，确保交易执行时所有可能访问的数据都已预取
		allAccounts := make([]common.Address, 0, len(r.accesses))
		for addr := range r.accesses {
			allAccounts = append(allAccounts, addr)
		}
		r.prefetchTrieData(db, allAccounts)
	})
}

// WaitForPrefetch waits for critical prefetch operations to complete,
// ensuring that account data is ready before transaction execution starts.
// This helps reduce execution time variance by ensuring data is preloaded.
func (r *BALReader) WaitForPrefetch() {
	// Wait for account resolutions to complete
	r.prestateReader.WaitForAccounts()
}

func (r *BALReader) prefetchTrieData(db *StateDB, addresses []common.Address) (int, int) {
	if db == nil || len(addresses) == 0 {
		return 0, 0
	}
	scheduleBALAccountPrefetch(db, addresses)
	var (
		storageAccounts int
		storageSlots    int
	)
	for _, addr := range addresses {
		access := r.accesses[addr]
		if access == nil {
			continue
		}
		acct := r.prestateReader.account(addr)
		if acct == nil || acct.Root == types.EmptyRootHash {
			continue
		}
		var slots []common.Hash
		// 预取 StorageChanges（写入的存储槽）
		for _, change := range access.StorageChanges {
			slots = append(slots, change.Slot)
		}
		// 预取 StorageReads（读取的存储槽），确保交易执行时数据已准备好
		slots = append(slots, access.StorageReads...)
		if len(slots) == 0 {
			continue
		}
		scheduleBALStoragePrefetch(db, addr, acct.Root, slots)
		storageAccounts++
		storageSlots += len(slots)
	}
	return storageAccounts, storageSlots
}

func (r *BALReader) ValidateStateReads(allReads bal.StateAccesses) error {
	// 1. remove any slots from 'allReads' which were written
	// 2. validate that the read set in the BAL matches 'allReads' exactly
	for addr, reads := range allReads {
		balAcctDiff := r.readAccountDiff(addr, len(r.block.Transactions())+2)
		if balAcctDiff != nil {
			for writeSlot := range balAcctDiff.StorageWrites {
				delete(reads, writeSlot)
			}
		}
		if _, ok := r.accesses[addr]; !ok {
			return fmt.Errorf("%x wasn't in BAL", addr)
		}

		expectedReads := r.accesses[addr].StorageReads
		if len(reads) != len(expectedReads) {
			return fmt.Errorf("mismatch between the number of computed reads and number of expected reads")
		}

		for _, slot := range expectedReads {
			if _, ok := reads[slot]; !ok {
				return fmt.Errorf("expected read is missing from BAL")
			}
		}
	}

	// TODO: where do we validate that the storage read/write sets are distinct?

	return nil
}

func (r *BALReader) AccessedState() (res map[common.Address]map[common.Hash]struct{}) {
	res = make(map[common.Address]map[common.Hash]struct{})
	for addr, accesses := range r.accesses {
		if len(accesses.StorageReads) > 0 {
			res[addr] = make(map[common.Hash]struct{})
			for _, slot := range accesses.StorageReads {
				res[addr][slot] = struct{}{}
			}
		} else if len(accesses.BalanceChanges) == 0 && len(accesses.NonceChanges) == 0 && len(accesses.StorageChanges) == 0 && len(accesses.CodeChanges) == 0 {
			res[addr] = make(map[common.Hash]struct{})
		}
	}
	return
}

// TODO: it feels weird that this modifies the prestate instance. However, it's needed because it will
// subsequently be used in Commit.
func (r *BALReader) StateRoot(prestate *StateDB) (root common.Hash, prestateLoadTime time.Duration, rootUpdateTime time.Duration) {
	lastIdx := len(r.block.Transactions()) + 1
	modifiedAccts := r.ModifiedAccounts()
	startPrestateLoad := time.Now()
	var (
		workerLimit = runtime.GOMAXPROCS(0)
		workers     errgroup.Group
	)
	if workerLimit > 0 {
		workers.SetLimit(workerLimit)
	}
	if prestate.prefetcher == nil && len(modifiedAccts) > 0 && !prestate.NoTries() {
		prestate.StartPrefetcher("bal-state-root", nil)
	}
	if prestate.prefetcher != nil {
		scheduleBALAccountPrefetch(prestate, modifiedAccts)
		if len(modifiedAccts) > 0 {
			log.Info("BAL trie account prefetch scheduled", "block", r.block.Number(), "accounts", len(modifiedAccts))
		}
	}
	resultBuf := len(modifiedAccts)
	if resultBuf == 0 {
		resultBuf = 1
	}
	type accountResult struct {
		address common.Address
		object  *stateObject
	}
	results := make(chan accountResult, resultBuf)
	for _, addr := range modifiedAccts {
		addr := addr
		workers.Go(func() error {
			diff := r.readAccountDiff(addr, lastIdx)
			acct := r.prestateReader.account(addr)
			obj, err := r.initMutatedObjFromDiff(prestate, addr, acct, diff)
			if err != nil {
				return err
			}
			if obj != nil {
				results <- accountResult{
					address: addr,
					object:  obj,
				}
			}
			return nil
		})
	}
	go func() {
		if err := workers.Wait(); err != nil {
			prestate.setError(fmt.Errorf("load prestate: %w", err))
		}
		close(results)
	}()
	for res := range results {
		prestate.setStateObject(res.object)
		prestate.journal.dirty(res.address)
	}
	if accounts, slots := r.prefetchTrieData(prestate, modifiedAccts); accounts > 0 {
		log.Info("BAL trie storage prefetch scheduled", "block", r.block.Number(), "accounts", accounts, "slots", slots)
	}
	prestateLoadTime = time.Since(startPrestateLoad)
	rootUpdateStart := time.Now()
	root = prestate.IntermediateRoot(true)
	rootUpdateTime = time.Since(rootUpdateStart)
	return root, prestateLoadTime, rootUpdateTime
}

func scheduleBALAccountPrefetch(prestate *StateDB, addresses []common.Address) {
	if prestate.prefetcher == nil || len(addresses) == 0 {
		return
	}
	addrs := make([]common.Address, len(addresses))
	copy(addrs, addresses)
	if err := prestate.prefetcher.prefetch(common.Hash{}, prestate.originalRoot, common.Address{}, addrs, nil, false); err != nil {
		log.Debug("Failed to prefetch BAL accounts", "count", len(addrs), "err", err)
	}
}

func scheduleBALStoragePrefetch(prestate *StateDB, addr common.Address, storageRoot common.Hash, slots []common.Hash) {
	if prestate.prefetcher == nil || len(slots) == 0 || storageRoot == (common.Hash{}) || storageRoot == types.EmptyRootHash {
		return
	}
	slotCopy := make([]common.Hash, len(slots))
	copy(slotCopy, slots)
	addrHash := crypto.Keccak256Hash(addr[:])
	if err := prestate.prefetcher.prefetch(addrHash, storageRoot, addr, nil, slotCopy, false); err != nil {
		log.Debug("Failed to prefetch BAL storage", "address", addr, "slots", len(slotCopy), "err", err)
	}
}

// changesAt returns all state changes at the given index.
func (r *BALReader) changesAt(idx int) *bal.StateDiff {
	res := &bal.StateDiff{Mutations: make(map[common.Address]*bal.AccountMutations)}
	for addr := range r.accesses {
		accountChanges := r.accountChangesAt(addr, idx)
		if accountChanges != nil {
			res.Mutations[addr] = accountChanges
		}
	}
	return res
}

// accountChangesAt returns the state changes of an account at a given index,
// or nil if there are no changes.
func (r *BALReader) accountChangesAt(addr common.Address, idx int) *bal.AccountMutations {
	acct, exist := r.accesses[addr]
	if !exist {
		return nil
	}

	var res bal.AccountMutations

	for i := len(acct.BalanceChanges) - 1; i >= 0; i-- {
		if acct.BalanceChanges[i].TxIdx == uint16(idx) {
			res.Balance = acct.BalanceChanges[i].Balance
		}
		if acct.BalanceChanges[i].TxIdx < uint16(idx) {
			break
		}
	}

	for i := len(acct.CodeChanges) - 1; i >= 0; i-- {
		if acct.CodeChanges[i].TxIdx == uint16(idx) {
			res.Code = acct.CodeChanges[i].Code
			break
		}
		if acct.CodeChanges[i].TxIdx < uint16(idx) {
			break
		}
	}

	for i := len(acct.NonceChanges) - 1; i >= 0; i-- {
		if acct.NonceChanges[i].TxIdx == uint16(idx) {
			res.Nonce = &acct.NonceChanges[i].Nonce
			break
		}
		if acct.NonceChanges[i].TxIdx < uint16(idx) {
			break
		}
	}

	for i := len(acct.StorageChanges) - 1; i >= 0; i-- {
		if res.StorageWrites == nil {
			res.StorageWrites = make(map[common.Hash]common.Hash)
		}
		slotWrites := acct.StorageChanges[i]

		for j := len(slotWrites.Accesses) - 1; j >= 0; j-- {
			if slotWrites.Accesses[j].TxIdx == uint16(idx) {
				res.StorageWrites[slotWrites.Slot] = slotWrites.Accesses[j].ValueAfter
				break
			}
			if slotWrites.Accesses[j].TxIdx < uint16(idx) {
				break
			}
		}
		if len(res.StorageWrites) == 0 {
			res.StorageWrites = nil
		}
	}

	if res.Code == nil && res.Nonce == nil && len(res.StorageWrites) == 0 && res.Balance == nil {
		return nil
	}
	return &res
}

func (r *BALReader) isModified(addr common.Address) bool {
	access, ok := r.accesses[addr]
	if !ok {
		return false
	}
	return len(access.StorageChanges) > 0 || len(access.BalanceChanges) > 0 || len(access.CodeChanges) > 0 || len(access.NonceChanges) > 0
}

func (r *BALReader) readAccount(db *StateDB, addr common.Address, idx int) *stateObject {
	diff := r.readAccountDiff(addr, idx)
	prestate := r.prestateReader.account(addr)
	return r.initObjFromDiff(db, addr, prestate, diff)
}

// readAccountDiff returns the accumulated state changes of an account up through idx.
func (r *BALReader) readAccountDiff(addr common.Address, idx int) *bal.AccountMutations {
	diff, exist := r.accesses[addr]
	if !exist {
		return nil
	}

	var res bal.AccountMutations

	for i := 0; i < len(diff.BalanceChanges) && diff.BalanceChanges[i].TxIdx <= uint16(idx); i++ {
		res.Balance = diff.BalanceChanges[i].Balance
	}

	for i := 0; i < len(diff.CodeChanges) && diff.CodeChanges[i].TxIdx <= uint16(idx); i++ {
		res.Code = diff.CodeChanges[i].Code
	}

	for i := 0; i < len(diff.NonceChanges) && diff.NonceChanges[i].TxIdx <= uint16(idx); i++ {
		res.Nonce = &diff.NonceChanges[i].Nonce
	}

	if len(diff.StorageChanges) > 0 {
		res.StorageWrites = make(map[common.Hash]common.Hash)
		for _, slotWrites := range diff.StorageChanges {
			for i := 0; i < len(slotWrites.Accesses) && slotWrites.Accesses[i].TxIdx <= uint16(idx); i++ {
				res.StorageWrites[slotWrites.Slot] = slotWrites.Accesses[i].ValueAfter
			}
		}
	}

	return &res
}

func (r *BALReader) ValidateStateDiffRange(startIdx int, endIdx int, computedDiff *bal.StateDiff) error {
	balChanges := &bal.StateDiff{Mutations: make(map[common.Address]*bal.AccountMutations)}
	for idx := startIdx; idx <= endIdx; idx++ {
		balChanges.Merge(r.changesAt(idx))
	}
	for addr, state := range balChanges.Mutations {
		computedAccountDiff, ok := computedDiff.Mutations[addr]
		if !ok {
			return fmt.Errorf("BAL %d-%d contained account %x which wasn't present in computed state diff", startIdx, endIdx, addr)
		}

		if !state.Eq(computedAccountDiff) {
			// 【添加详细日志】
			log.Error("=== BAL value mismatch ===",
				"startIdx", startIdx,
				"endIdx", endIdx,
				"address", addr.Hex(),
				"block", r.block.Number())

			// 比较 Balance
			if state.Balance != nil || computedAccountDiff.Balance != nil {
				balBalance := "nil"
				if state.Balance != nil {
					balBalance = state.Balance.String()
				}
				computedBalance := "nil"
				if computedAccountDiff.Balance != nil {
					computedBalance = computedAccountDiff.Balance.String()
				}
				log.Error("  Balance mismatch",
					"startIdx", startIdx,
					"endIdx", endIdx,
					"address", addr.Hex(),
					"BAL", balBalance,
					"computed", computedBalance)
			}

			// 比较 Nonce
			if state.Nonce != nil || computedAccountDiff.Nonce != nil {
				balNonce := "nil"
				if state.Nonce != nil {
					balNonce = fmt.Sprintf("%d", *state.Nonce)
				}
				computedNonce := "nil"
				if computedAccountDiff.Nonce != nil {
					computedNonce = fmt.Sprintf("%d", *computedAccountDiff.Nonce)
				}
				log.Error("  Nonce mismatch",
					"startIdx", startIdx,
					"endIdx", endIdx,
					"address", addr.Hex(),
					"BAL", balNonce,
					"computed", computedNonce)
			}

			// 比较 Storage
			balStorageCount := 0
			if state.StorageWrites != nil {
				balStorageCount = len(state.StorageWrites)
			}
			computedStorageCount := 0
			if computedAccountDiff.StorageWrites != nil {
				computedStorageCount = len(computedAccountDiff.StorageWrites)
			}
			if balStorageCount != computedStorageCount {
				log.Error("  Storage count mismatch",
					"startIdx", startIdx,
					"endIdx", endIdx,
					"address", addr.Hex(),
					"BAL_count", balStorageCount,
					"computed_count", computedStorageCount)
			}

			return fmt.Errorf("difference between computed state diff and BAL %d-%d entry for account %x", startIdx, endIdx, addr)
		}
	}

	if len(balChanges.Mutations) != len(computedDiff.Mutations) {
		log.Error("Account count mismatch", "startIdx", startIdx,
			"endIdx", endIdx,
			"BAL_count", len(balChanges.Mutations),
			"computed_count", len(computedDiff.Mutations))

		balAccounts := make(map[common.Address]bool)
		for addr := range balChanges.Mutations {
			balAccounts[addr] = true
			log.Error("  BAL has", "startIdx", startIdx,
				"endIdx", endIdx,
				"address", addr.Hex())
		}

		for addr := range computedDiff.Mutations {
			if !balAccounts[addr] {
				log.Error("  Computed has (NOT in BAL)", "startIdx", startIdx,
					"endIdx", endIdx,
					"address", addr.Hex())
			}
		}

		return fmt.Errorf("computed state diff contained mutated accounts which weren't reported in BAL %d-%d", startIdx, endIdx)
	}

	return nil
}

// ValidateStateDiff returns an error if the computed state diff is not equal to
// diff reported from the access list at the given index.
func (r *BALReader) ValidateStateDiff(idx int, computedDiff *bal.StateDiff) error {
	return r.ValidateStateDiffRange(idx, idx, computedDiff)
}
