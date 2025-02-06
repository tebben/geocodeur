package bktree

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/texttheater/golang-levenshtein/levenshtein"
)

// BKTreeNode represents a node in the BK-Tree
type BKTreeNode struct {
	Value    string
	Children map[int]*BKTreeNode
}

// BKTree represents the BK-Tree structure
type BKTree struct {
	Root *BKTreeNode
	DB   *badger.DB
}

func NewBKTree(dbPath string) (*BKTree, error) {
	// Open BadgerDB for storage
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BKTree{DB: db}, nil
}

// Insert adds a value to the BK-Tree
func (tree *BKTree) Insert(value string) error {
	if tree.Root == nil {
		tree.Root = &BKTreeNode{Value: value, Children: make(map[int]*BKTreeNode)}
	} else {
		tree.insertNode(tree.Root, value)
	}
	// Store the value in BadgerDB (key: hash of the value)
	err := tree.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(value), []byte(value)) // Simplified storage
	})
	return err
}

// insertNode recursively inserts a value into the tree
func (tree *BKTree) insertNode(node *BKTreeNode, value string) {
	distance := levenshtein.DistanceForStrings([]rune(node.Value), []rune(value), levenshtein.DefaultOptions)
	if _, exists := node.Children[distance]; exists {
		tree.insertNode(node.Children[distance], value)
	} else {
		node.Children[distance] = &BKTreeNode{Value: value, Children: make(map[int]*BKTreeNode)}
	}
}

// Search performs a fuzzy search with a distance threshold
func (tree *BKTree) Search(query string, maxDistance int) ([]string, error) {
	var results []string

	// If the tree is empty (no data inserted yet), we should load from DB or return early
	if tree.Root == nil {
		// Optionally, load data from BadgerDB if necessary
		fmt.Print("Loading from DB")
		err := tree.loadFromDB()
		if err != nil {
			return nil, err
		}
	}

	fmt.Print("Searching")

	err := tree.DB.View(func(txn *badger.Txn) error {
		return tree.searchNode(tree.Root, query, maxDistance, &results)
	})
	return results, err
}

// searchNode recursively searches the BK-Tree
func (tree *BKTree) searchNode(node *BKTreeNode, query string, maxDistance int, results *[]string) error {
	distance := levenshtein.DistanceForStrings([]rune(node.Value), []rune(query), levenshtein.DefaultOptions)
	if distance <= maxDistance {
		*results = append(*results, node.Value)
	}
	for dist, childNode := range node.Children {
		if dist <= maxDistance+distance {
			err := tree.searchNode(childNode, query, maxDistance, results)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tree *BKTree) loadFromDB() error {
	// Create a new iterator to go through all key-value pairs in the DB
	err := tree.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Iterate over the entire DB
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil) // Get the value of the current item
			if err != nil {
				return err
			}

			// Rebuild the tree from the stored data
			if tree.Root == nil {
				tree.Root = &BKTreeNode{Value: string(value), Children: make(map[int]*BKTreeNode)}
			} else {
				tree.insertNode(tree.Root, string(value))
			}
		}
		return nil
	})
	return err
}

/*
const DEFAULT_MAX_LEVENSHTEIN = 50

type bktreeNode struct {
	str   string
	child []*bktreeNode
}

func newBktreeNode(s string, limit int) *bktreeNode {
	return &bktreeNode{
		str:   s,
		child: make([]*bktreeNode, limit+1),
	}
}

type BKTree struct {
	root             *bktreeNode
	size             int
	levenshteinLimit int
}

func New() *BKTree {
	return &BKTree{
		root:             nil,
		size:             0,
		levenshteinLimit: DEFAULT_MAX_LEVENSHTEIN,
	}
}

func (this *BKTree) SetLevenshteinLimit(limit int) {
	this.levenshteinLimit = limit
}

func (this *BKTree) GetLevenshteinLimit() int {
	return this.levenshteinLimit
}

func (this *BKTree) Size() int {
	return this.size
}

func (this *BKTree) insert(rt *bktreeNode, s string) bool {
	d := Levenshtein(rt.str, s)
	if d > this.levenshteinLimit || d >= len(rt.child) {
		return false
	}

	if rt.child[d] == nil {
		rt.child[d] = newBktreeNode(s, this.levenshteinLimit)
		return true
	} else {
		return this.insert(rt.child[d], s)
	}
}

func (this *BKTree) Insert(s string) bool {
	if this.root == nil {
		this.root = newBktreeNode(s, this.levenshteinLimit)
		this.size++
		return true
	}

	if this.insert(this.root, s) {
		this.size++
		return true
	}

	return false
}

func (this *BKTree) find(rt *bktreeNode, s string, k int, n int) (ret []string) {
	if n == 0 {
		return []string{}
	}

	d := Levenshtein(rt.str, s)
	if d <= k {
		ret = append(ret, rt.str)
		if n >= 0 && len(ret) >= n {
			return ret[0:n]
		}
	}

	dx, dy := max(0, d-k), min(d+k, len(rt.child)-1)
	for i := dx; i <= dy; i++ {
		if rt.child[i] != nil {
			ret = append(ret, this.find(rt.child[i], s, k, n)...)
			if n >= 0 && len(ret) >= n {
				return ret[0:n]
			}
		}
	}
	return ret
}

// if n < 0, there is no limit on the number of find strings.
func (this *BKTree) Find(s string, k int, n int) []string {
	if this.root == nil {
		return []string{}
	}
	return this.find(this.root, s, k, n)
}

func (this *BKTree) Levenshtein(s1, s2 string) int {
	return Levenshtein(s1, s2)
}

func Levenshtein(s1, s2 string) int {
	runes1 := []rune(s1)
	runes2 := []rune(s2)

	m := len(runes1)
	n := len(runes2)

	// roll array
	d := make([][]int, 2)
	d[0] = make([]int, n+1)
	d[1] = make([]int, n+1)

	turn, pre := 0, 0
	for i := 0; i <= n; i++ {
		d[turn][i] = i
	}
	for i := 1; i <= m; i++ {
		pre = turn
		turn = (turn + 1) % 2
		d[turn][0] = i

		for j := 1; j <= n; j++ {
			if runes1[i-1] == runes2[j-1] {
				d[turn][j] = d[pre][j-1]
			} else {
				d[turn][j] = min(min(d[pre][j]+1, d[turn][j-1]+1), d[pre][j-1]+1)
			}
		}
	}

	return d[turn][n]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
*/
