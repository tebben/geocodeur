package bktree

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/texttheater/golang-levenshtein/levenshtein"
)

func openBadgerDB() (*badger.DB, error) {
	opts := badger.DefaultOptions("./badgerdb")
	opts.Dir = "./badgerdb"
	opts.ValueDir = "./badgerdb"

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("could not open Badger DB: %v", err)
	}
	return db, nil
}

func storeString(db *badger.DB, key, value string) error {
	return db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		if err != nil {
			return fmt.Errorf("could not store value in BadgerDB: %v", err)
		}
		return nil
	})
}

func createNGramIndex(db *badger.DB, value string, key string, n int) error {
	grams := make(map[string][]string)

	for i := 0; i <= len(value)-n; i++ {
		gram := value[i : i+n]
		grams[gram] = append(grams[gram], key)
	}

	return db.Update(func(txn *badger.Txn) error {
		for gram, keys := range grams {
			gramKey := []byte(gram)
			// Store list of keys for each n-gram
			for _, key := range keys {
				txn.Set(gramKey, []byte(key))
			}
		}
		return nil
	})
}

func fuzzySearch(db *badger.DB, query string, threshold int) ([]string, error) {
	var results []string

	err := db.View(func(txn *badger.Txn) error {
		// Iterate over all key-value pairs in BadgerDB
		err := txn.ForEach(func(key []byte, value []byte) error {
			// Calculate Levenshtein distance
			distance := levenshtein.ComputeDistance(query, string(value))
			if distance <= threshold {
				results = append(results, string(value))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("could not iterate over Badger DB: %v", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

func buildBKTree(db *badger.DB) (*bk.BKTree, error) {
	tree := bk.NewTree(levenshtein.ComputeDistance)

	err := db.View(func(txn *badger.Txn) error {
		err := txn.ForEach(func(key []byte, value []byte) error {
			tree.Add(string(value), nil) // Add each value to BK-tree
			return nil
		})
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("could not populate BK-tree: %v", err)
	}
	return tree, nil
}

func searchBKTree(tree *bk.BKTree, query string, threshold int) ([]string, error) {
	results := make([]string, 0)

	tree.Visit(query, threshold, func(value string, _ interface{}) {
		results = append(results, value)
	})

	return results, nil
}
