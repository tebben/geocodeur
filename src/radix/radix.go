package radix

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

type RadixNode struct {
	Prefix   string
	Children map[string]*RadixNode
}

type RadixTree struct {
	DB *badger.DB
}

func NewRadixTree(dbPath string) (*RadixTree, error) {
	// Open BadgerDB
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable logging for production
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &RadixTree{DB: db}, nil
}

func (tree *RadixTree) Close() {
	tree.DB.Close()
}

// Hash function to generate unique keys for BadgerDB
func generateKey(value string) []byte {
	h := fnv.New64a()
	h.Write([]byte(value))
	return []byte(fmt.Sprintf("%x", h.Sum(nil)))
}

func (node *RadixNode) AddChild(child *RadixNode) {
	if node.Children == nil {
		node.Children = make(map[string]*RadixNode)
	}
	node.Children[child.Prefix] = child
}

func (tree *RadixTree) Insert(value string) error {
	// Start with an empty RadixNode
	root := &RadixNode{Prefix: ""}

	// Split the value into parts (or you can use the entire string as one part)
	parts := strings.Split(value, " ")

	for _, part := range parts {
		// Look for an existing child
		if child, ok := root.Children[part]; ok {
			// Continue down the existing child
			root = child
		} else {
			// Insert a new child if not found
			child := &RadixNode{Prefix: part}
			root.AddChild(child)
			root = child
		}
	}

	// Generate a unique key for BadgerDB using the value
	key := generateKey(value)

	// Store the value in BadgerDB
	err := tree.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte(value))
	})

	return err
}

func (tree *RadixTree) Search(value string) (string, error) {
	var result string

	// Generate the key for the search value
	key := generateKey(value)

	// Retrieve the value from BadgerDB
	err := tree.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			result = string(val)
			return nil
		})
		return err
	})

	if err != nil {
		return "", err
	}

	return result, nil
}
