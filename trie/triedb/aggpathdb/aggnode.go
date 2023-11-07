package aggpathdb

import (
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// AggNode is a basic structure for aggregate and store two layer trie node.
type AggNode struct {
	nodes map[string]*trienode.Node
}

func NewAggNode() *AggNode {
	return &AggNode{
		nodes: make(map[string]*trienode.Node),
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

func (n *AggNode) copy() (*AggNode, error) {
	return DecodeAggNode(n.encodeTo())
}

func (n *AggNode) Empty() bool {
	return len(n.nodes) == 0
}

func (n *AggNode) Size() int {
	size := 0
	for k, node := range n.nodes {
		size += len(k) + len(node.Blob)
	}
	return size
}

func (n *AggNode) Update(path []byte, node *trienode.Node) {
	n.nodes[index(path)] = node
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

func (n *AggNode) Node(path []byte) *trienode.Node {
	tn, ok := n.nodes[index(path)]
	if !ok {
		return nil
	}
	if tn.Hash == (common.Hash{}) && len(tn.Blob) != 0 {
		h := newHasher()
		defer h.release()
		tn.Hash = h.hash(tn.Blob)
	}
	return tn
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
			node *trienode.Node
		)
		key, rest, err = decodeKey(rest)
		if err != nil {
			return fmt.Errorf("decode node key failed in AggNode: %v", err)
		}
		node, rest, err = decodeRawNode(rest)
		if err != nil {
			return fmt.Errorf("decode node key failed in AggNode: %v", err)
		}
		n.nodes[string(key)] = node
		if len(rest) == 0 {
			break
		}
	}
	return nil
}

func (n *AggNode) encodeTo() []byte {
	w := rlp.NewEncoderBuffer(nil)
	offset := w.List()

	for k, tn := range n.nodes {
		w.WriteBytes([]byte(k))
		writeRawNode(w, tn.Blob)
	}
	w.ListEnd(offset)
	result := w.ToBytes()
	w.Flush()
	return result
}

func writeRawNode(w rlp.EncoderBuffer, n []byte) {
	if n == nil {
		w.Write(rlp.EmptyString)
	} else {
		w.WriteBytes(n)
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

func decodeRawNode(buf []byte) (*trienode.Node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}

	if kind == rlp.String && len(val) == 0 {
		return nil, rest, nil
	}

	// Hashes are not calculated here to avoid unnecessary overhead
	return &trienode.Node{Blob: val}, rest, nil
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
		rawdb.DeleteAccountTrieNode(db, aggPath)
	} else {
		rawdb.DeleteStorageTrieNode(db, owner, aggPath)
	}
}

func loadAggNodeFromDatabase(db ethdb.KeyValueReader, owner common.Hash, aggPath []byte) (*AggNode, error) {
	var blob []byte
	if owner == (common.Hash{}) {
		blob = rawdb.ReadAccountTrieAggNode(db, aggPath)
	} else {
		blob = rawdb.ReadStorageTrieAggNode(db, owner, aggPath)
	}

	if blob == nil {
		return nil, nil
	}

	return DecodeAggNode(blob)
}

func ReadTrieNodeFromAggNode(reader ethdb.KeyValueReader, owner common.Hash, path []byte) ([]byte, common.Hash) {
	aggPath := ToAggPath(path)
	aggNode, err := loadAggNodeFromDatabase(reader, owner, aggPath)
	if err != nil {
		panic(fmt.Sprintf("Decode account trie node failed. error: %v", err))
	}
	if aggNode == nil {
		return nil, common.Hash{}
	}

	node := aggNode.Node(path)

	return node.Blob, node.Hash
}

func DeleteTrieNodeFromAggNode(writer ethdb.KeyValueWriter, reader ethdb.KeyValueReader, owner common.Hash, path []byte) {
	aggPath := ToAggPath(path)
	aggNode, err := loadAggNodeFromDatabase(reader, owner, aggPath)
	if err != nil {
		panic(fmt.Sprintf("Decode account trie node failed. error: %v", err))
	}
	if aggNode == nil {
		return
	}
	aggNode.Delete(path)

	if aggNode.Empty() {
		deleteAggNode(writer, owner, aggPath)
	} else {
		writeAggNode(writer, owner, aggPath, aggNode.encodeTo())
	}
}

func WriteTrieNodeWithAggNode(writer ethdb.KeyValueWriter, reader ethdb.KeyValueReader, owner common.Hash, path []byte, node []byte) {
	aggPath := ToAggPath(path)
	aggNode, err := loadAggNodeFromDatabase(reader, owner, aggPath)
	if err != nil {
		panic(fmt.Sprintf("Decode account trie node failed. error: %v", err))
	}
	if aggNode == nil {
		return
	}
	aggNode.Update(path, &trienode.Node{Blob: node})

	writeAggNode(writer, owner, aggPath, aggNode.encodeTo())
}

func HasTrieNodeInAggNode(db ethdb.KeyValueReader, owner common.Hash, path []byte) bool {
	aggPath := ToAggPath(path)
	aggNode, err := loadAggNodeFromDatabase(db, owner, aggPath)
	if err != nil {
		panic(fmt.Sprintf("Decode account trie node failed. error: %v", err))
	}
	if aggNode == nil {
		return false
	}
	return aggNode.Has(path)
}
