package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/meili"
	"github.com/tebben/geocodeur/preprocess"
	"github.com/tebben/geocodeur/server"
	"github.com/tebben/geocodeur/settings"
)

func initLogger(config settings.Config) {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	if config.Server.Debug {
		log.SetLevel(log.DebugLevel)
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
}

func main() {
	if len(os.Args) < 1 {
		log.Fatal("No command provided")
	}

	err := settings.InitializeConfig()
	if err != nil {
		log.Fatal(err)
	}

	config := settings.GetConfig()
	initLogger(config)

	command := os.Args[1]
	if command == "create" {
		meili.Setup(config)
	} else if command == "query" {
		query(config)
	} else if command == "process" {
		process()
	} else if command == "server" {
		server.Start(config)
	} else {
		log.Fatalf("Unknown command")
	}
}

func query(config settings.Config) {
	meili.Query(os.Args[2])
	/* geocodeOptions := service.NewGeocodeOptions(10, nil, false)

	timeStart := time.Now()
	geocodeResults, err := service.Geocode(geocodeOptions, os.Args[2])
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}

	timeEnd := time.Now()

	for _, result := range geocodeResults {
		log.Infof("ID: %v, Name: %s, Class: %s, Subclass: %s, Relations: %v, Sim: %v", result.ID, result.Name, result.Class, result.Subclass, result.Relations, result.Similarity)
	}

	log.Infof("-----------\n")
	log.Infof("%v", timeEnd.Sub(timeStart)) */
}

func process() {
	preprocess.ProcessAll()
}

// Levenshtein calculates the minimum number of single-character edits
// (insertions, deletions, or substitutions) required to change one string into the other.
func Levenshtein(s1, s2 string) int {
	m := len(s1)
	n := len(s2)
	dp := make([][]int, m+1)

	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// Initialize the base case
	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}

	// Fill the table
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			cost := 0
			if s1[i-1] != s2[j-1] {
				cost = 1
			}
			dp[i][j] = min(dp[i-1][j-1]+cost, min(dp[i-1][j]+1, dp[i][j-1]+1))
		}
	}

	return dp[m][n]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// BKTree node
type BKTree struct {
	word     string
	children map[int]*BKTree
}

// NewBKTree creates a new BKTree node.
func NewBKTree(word string) *BKTree {
	return &BKTree{
		word:     word,
		children: make(map[int]*BKTree),
	}
}

// Add adds a word to the BK-tree based on Levenshtein distance.
func (node *BKTree) Add(word string) {
	dist := Levenshtein(node.word, word)
	if child, exists := node.children[dist]; exists {
		child.Add(word)
	} else {
		node.children[dist] = NewBKTree(word)
	}
}

// Search finds words within a given radius of the query word.
func (node *BKTree) Search(query string, radius int, results *[]string) {
	dist := Levenshtein(node.word, query)
	if dist <= radius {
		*results = append(*results, node.word)
	}

	// Search the children if they are within the radius
	for childDist, child := range node.children {
		if dist-childDist <= radius && dist+childDist >= radius {
			child.Search(query, radius, results)
		}
	}
}
