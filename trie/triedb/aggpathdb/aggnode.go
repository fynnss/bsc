package aggpathdb

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var NonEmptyHash = common.BytesToHash([]byte("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"))

// AggNode is a basic structure for aggregate and store two layer trie node.
type AggNode struct {
	nodes map[string][]byte
}

func NewAggNode() *AggNode {
	return &AggNode{
		nodes: make(map[string][]byte),
	}
}

func DecodeAggNode(data []byte) (*AggNode, error) {
	aggNode := NewAggNode()
	err := aggNode.decodeFrom(data)
	if err != nil {
		return nil, err
	}
	return aggNode, nil
}

func ToAggPath(path []byte) []byte {
	if len(path)%2 == 0 {
		// even path
		return path
	} else {
		// odd path
		return path[:len(path)-1]
	}
}

func index(path []byte) string {
	if len(path)%2 == 0 {
		return ""
	} else {
		return string(path[len(path)-1])
	}
}

func indexBytes(path []byte) []byte {
	if len(path)%2 == 0 {
		return nil
	} else {
		return path[len(path)-1:]
	}
}

func (n *AggNode) copy() (*AggNode, error) {
	return DecodeAggNode(n.encodeTo())
}

func (n *AggNode) Empty() bool {
	return len(n.nodes) == 0
}

func (n *AggNode) Size() int {
	size := 0
	for k, node := range n.nodes {
		size += len(k) + len(node)
	}
	return size
}

func (n *AggNode) Update(path []byte, blob []byte) {
	n.nodes[index(path)] = blob
}

func (n *AggNode) Delete(path []byte) {
	key := index(path)
	_, ok := n.nodes[key]
	if ok {
		delete(n.nodes, key)
	}
}

func (n *AggNode) Has(path []byte) bool {
	_, ok := n.nodes[index(path)]
	return ok
}

func (n *AggNode) Node(path []byte) ([]byte, common.Hash) {
	blob, ok := n.nodes[index(path)]
	if !ok {
		return nil, common.Hash{}
	}
	if len(blob) != 0 {
		h := newHasher()
		defer h.release()
		return blob, h.hash(blob)
	}
	return nil, common.Hash{}
}

func (n *AggNode) decodeFrom(buf []byte) error {
	if len(buf) == 0 {
		return io.ErrUnexpectedEOF
	}

	rest, _, err := rlp.SplitList(buf)
	if err != nil {
		return fmt.Errorf("decode error: %v", err)
	}
	if len(rest) == 0 {
		return nil
	}

	for {
		var (
			key  []byte
			blob []byte
		)
		key, rest, err = decodeKey(rest)
		if err != nil {
			return fmt.Errorf("decode node key failed in AggNode: %v", err)
		}
		blob, rest, err = decodeRawNode(rest)
		if err != nil {
			return fmt.Errorf("decode raw node failed in AggNode: %v", err)
		}
		n.nodes[string(key)] = blob
		if len(rest) == 0 {
			break
		}
	}
	return nil
}

func (n *AggNode) encodeTo() []byte {
	w := rlp.NewEncoderBuffer(nil)
	offset := w.List()

	for k, blob := range n.nodes {
		writeRawKey(w, []byte(k))
		writeRawNode(w, blob)
	}
	w.ListEnd(offset)
	result := w.ToBytes()
	w.Flush()
	return result
}

func AggNodeString(blob []byte) string {
	builder := strings.Builder{}

	rest, _, err := rlp.SplitList(blob)
	if err != nil {
		return ""
	}
	if len(rest) == 0 {
		return ""
	}

	for {
		var (
			key   []byte
			nBlob []byte
		)
		key, rest, err = decodeKey(rest)
		if err != nil {
			return ""
		}
		nBlob, rest, err = decodeRawNode(rest)
		if err != nil {
			return ""
		}
		h := newHasher()
		nHash := h.hash(nBlob)
		h.release()

		builder.WriteString(fmt.Sprintf("%s: %v, ", common.Bytes2Hex(key), nHash.String()))
		if len(rest) == 0 {
			break
		}
	}
	return builder.String()
}

func ReadFromBlob(path []byte, blob []byte) ([]byte, common.Hash, error) {
	if len(blob) == 0 {
		return nil, common.Hash{}, io.ErrUnexpectedEOF
	}

	rest, _, err := rlp.SplitList(blob)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("decode error: %v", err)
	}
	if len(rest) == 0 {
		return nil, common.Hash{}, nil
	}

	sKey := indexBytes(path)
	for {
		var (
			key   []byte
			nBlob []byte
		)
		key, rest, err = decodeKey(rest)
		if err != nil {
			return nil, common.Hash{}, fmt.Errorf("decode node key failed in AggNode: %v", err)
		}
		nBlob, rest, err = decodeRawNode(rest)
		if err != nil {
			return nil, common.Hash{}, fmt.Errorf("decode raw node failed in AggNode: %v", err)
		}
		if bytes.Compare(key, sKey) == 0 {
			h := newHasher()
			nHash := h.hash(nBlob)
			h.release()
			return nBlob, nHash, nil
		}
		if len(rest) == 0 {
			break
		}
	}
	return nil, common.Hash{}, nil
}

func UpdateToBlob(blob []byte, nodes map[string]*trienode.Node) ([]byte, error) {
	// init rlp encoder
	w := rlp.NewEncoderBuffer(nil)
	offset := w.List()

	cnt := 0
	excludeList := make(map[string]struct{})
	for path, n := range nodes {
		k := indexBytes([]byte(path))
		if !n.IsDeleted() {
			writeRawKey(w, k)
			writeRawNode(w, n.Blob)
			cnt++
		}
		excludeList[string(k)] = struct{}{}
	}
	// decode the blob
	if len(blob) != 0 {
		rest, _, err := rlp.SplitList(blob)
		if err != nil {
			return nil, fmt.Errorf("decode error: %v", err)
		}
		for {
			var (
				key   []byte
				nBlob []byte
			)
			key, rest, err = decodeKey(rest)
			if err != nil {
				return nil, fmt.Errorf("decode node key failed in AggNode: %v", err)
			}

			nBlob, rest, err = decodeRawNode(rest)
			if err != nil {
				return nil, fmt.Errorf("decode raw node in AggNode: %v", err)
			}
			_, ok := excludeList[string(key)]
			if !ok {
				// keep
				writeRawKey(w, key)
				writeRawNode(w, nBlob)
				cnt++
			}
			if len(rest) == 0 {
				break
			}
		}
	}

	if cnt == 0 {
		return nil, nil
	} else {
		w.ListEnd(offset)
		result := w.ToBytes()
		w.Flush()
		return result, nil
	}
}

func writeRawNode(w rlp.EncoderBuffer, n []byte) {
	if n == nil {
		w.Write(rlp.EmptyString)
	} else {
		w.WriteBytes(n)
	}
}

func writeRawKey(w rlp.EncoderBuffer, k []byte) {
	if k == nil {
		w.Write(rlp.EmptyString)
	} else {
		w.WriteBytes(k)
	}
}

func decodeKey(buf []byte) ([]byte, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}

	if kind == rlp.String && len(val) == 0 {
		return nil, rest, nil
	}

	// Hashes are not calculated here to avoid unnecessary overhead
	return val, rest, nil
}

func decodeRawNode(buf []byte) ([]byte, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}

	if kind == rlp.String && len(val) == 0 {
		return nil, rest, nil
	}

	// Hashes are not calculated here to avoid unnecessary overhead
	return val, rest, nil
}

func writeAggNode(db ethdb.KeyValueWriter, owner common.Hash, aggPath []byte, aggNodeBytes []byte) {
	if owner == (common.Hash{}) {
		rawdb.WriteAccountTrieAggNode(db, aggPath, aggNodeBytes)
	} else {
		rawdb.WriteStorageTrieAggNode(db, owner, aggPath, aggNodeBytes)
	}
}

func deleteAggNode(db ethdb.KeyValueWriter, owner common.Hash, aggPath []byte) {
	if owner == (common.Hash{}) {
		rawdb.DeleteAccountTrieAggNode(db, aggPath)
	} else {
		rawdb.DeleteStorageTrieAggNode(db, owner, aggPath)
	}
}

func loadAggNodeFromDatabase(db ethdb.KeyValueReader, owner common.Hash, aggPath []byte) []byte {
	if owner == (common.Hash{}) {
		return rawdb.ReadAccountTrieAggNode(db, aggPath)
	} else {
		return rawdb.ReadStorageTrieAggNode(db, owner, aggPath)
	}
}

func ReadTrieNodeFromAggNode(reader ethdb.KeyValueReader, owner common.Hash, path []byte) ([]byte, common.Hash) {
	blob := loadAggNodeFromDatabase(reader, owner, ToAggPath(path))

	nBlob, nHash, err := ReadFromBlob(path, blob)
	if err != nil {
		return nil, common.Hash{}
	}

	return nBlob, nHash
}

func DeleteTrieNodeFromAggNode(writer ethdb.KeyValueWriter, reader ethdb.KeyValueReader, owner common.Hash, path []byte) {
	aggPath := ToAggPath(path)
	blob := loadAggNodeFromDatabase(reader, owner, aggPath)

	if blob == nil {
		return
	}

	newBlob, err := UpdateToBlob(blob, map[string]*trienode.Node{string(path): trienode.NewDeleted()})
	if err != nil {
		return
	}

	if newBlob == nil {
		deleteAggNode(writer, owner, aggPath)
	} else {
		writeAggNode(writer, owner, aggPath, newBlob)
	}
}

func WriteTrieNodeWithAggNode(writer ethdb.KeyValueWriter, reader ethdb.KeyValueReader, owner common.Hash, path []byte, node []byte) {
	aggPath := ToAggPath(path)
	blob := loadAggNodeFromDatabase(reader, owner, aggPath)

	if blob == nil {
		return
	}

	newBlob, err := UpdateToBlob(blob, map[string]*trienode.Node{string(path): trienode.New(NonEmptyHash, node)})
	if err != nil {
		return
	}

	if newBlob == nil {
		deleteAggNode(writer, owner, aggPath)
	} else {
		writeAggNode(writer, owner, aggPath, newBlob)
	}
}

func HasTrieNodeInAggNode(db ethdb.KeyValueReader, owner common.Hash, path []byte) bool {
	blob := loadAggNodeFromDatabase(db, owner, ToAggPath(path))
	nBlob, _, err := ReadFromBlob(path, blob)
	if err != nil {
		return false
	}

	if nBlob == nil {
		return true
	}
	return false
}
