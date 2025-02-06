package meili

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis/analyzer"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search/similarity"
	"github.com/tebben/geocodeur/bktree"
	"github.com/tebben/geocodeur/settings"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	log "github.com/sirupsen/logrus"
)

// put it here for now, move to settings and load
var TABLE_OVERTURE = "overture"
var TABLE_SEARCH = "overture_search"

/*
	 var aliases = map[string]string{
		"'s-Hertogenbosch": "Den Bosch",
	}
*/
var truncations = []string{
	"Rijksweg",
}

var counter uint64

type Record struct {
	ID       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Geom     string `parquet:"name=geom, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Centroid string `parquet:"name=centroid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Class    string `parquet:"name=class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Subclass string `parquet:"name=subclass, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Relation string `parquet:"name=relation, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
}

func getNextID() uint64 {
	return atomic.AddUint64(&counter, 1)
}

func Setup(config settings.Config) {
	/* defaultConfig := defaultConfig()
	writer, err := bluge.OpenWriter(defaultConfig)
	if err != nil {
		log.Fatalf("error opening writer: %v", err)
	}
	defer writer.Close()
	create(writer, config) */

	tree, err := bktree.NewBKTree("./badgerdb")
	if err != nil {
		fmt.Println("Error opening BadgerDB:", err)
		return
	}
	defer tree.DB.Close()

	//create(tree, config)

	/* entries := []string{"Amsterdam", "Kerkstraat Amsterdam", "Utrecht", "kerkstraat", "Rotterdam", "Nieuw-Amsterdam"}
	for _, entry := range entries {
		err = tree.Insert(preprocess(entry))
		if err != nil {
			fmt.Println("Error inserting:", err)
			return
		}
	} */

	timeStart := time.Now()
	result, err := tree.Search(preprocess("kerkstr amsterdam"), 5)
	if err != nil {
		fmt.Println("Error searching:", err)
		return
	}

	fmt.Println("Search result:", result)
	fmt.Printf("Time taken: %v\n", time.Now().Sub(timeStart))

	/* bk := bktree.New()

	for _, entry := range entries {
		ow := preprocess(entry)
		log.Infof("Inserting %s\n", ow)
		bk.Insert(ow)
	}
	bk.SetLevenshteinLimit(50)

	query := "kerkstraat"
	query = preprocess(query)

	timeStart := time.Now()
	ret := bk.Find(query, 3, 10)

	fmt.Println(ret) */

}

func preprocess(s string) string {
	s = strings.ToLower(s)             // Convert to lowercase
	words := strings.Fields(s)         // Split the string into words
	sort.Strings(words)                // Sort the words alphabetically
	output := strings.Join(words, " ") // Join the sorted words back together

	return output
}

func defaultConfig() bluge.Config {
	blugeConfig := bluge.DefaultConfig("../data/bluge")
	blugeConfig.DefaultSearchField = "alias"
	blugeConfig.DefaultSearchAnalyzer = analyzer.NewStandardAnalyzer()
	blugeConfig.DefaultSimilarity = similarity.NewBM25Similarity() //similarity.NewBM25SimilarityBK1(0.9, 0.8)

	return blugeConfig
}

func Query(query string) {
	config := defaultConfig()

	reader, err := bluge.OpenReader(config)
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	timeStart := time.Now()

	/*
		Increase b: A higher b might boost the impact of length normalization,
		penalizing terms like "Amsterdam" that are short and have exact
		matches, giving more weight to "Nieuw-Amsterdam."

		Adjust k1: Decreasing k1 will reduce the term frequency's impact, making
		the results less sensitive to repeated exact matches and potentially
		favoring partial matches.
	*/

	//q := bluge.NewMatchQuery(query)
	//q.SetOperator(bluge.MatchQueryOperatorAnd)
	//q.SetField("alias")

	q := bluge.NewBooleanQuery().
		AddMust(bluge.NewFuzzyQuery(query).SetField("alias")).
		AddShould(bluge.NewFuzzyQuery(query).SetField("relation"))

	req := bluge.NewTopNSearch(10, q)
	//req.SortBy([]string{"+classRank", "+wordCount", "+subclassRank", "+charCount"})
	req.SortBy([]string{"+classRank", "+subclassRank"})

	dmi, err := reader.Search(context.Background(), req)
	if err != nil {
		log.Fatalf("error executing search: %v", err)
	}

	timeEnd := time.Now()

	next, err := dmi.Next()
	for err == nil && next != nil {
		var id string
		var name string
		var class string
		var subclass string

		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				id = string(value)
			}

			if field == "alias" {
				name = string(value)
			}

			if field == "class" {
				class = string(value)
			}

			if field == "subclass" {
				subclass = string(value)
			}

			return true
		})

		if err != nil {
			log.Fatalf("error accessing stored fields: %v", err)
		}

		log.Infof("id: %s, name: %s, class: %s, subclass: %s", id, name, class, subclass)
		next, err = dmi.Next()
	}
	if err != nil {
		log.Fatalf("error iterating results: %v", err)
	}

	err = reader.Close()
	if err != nil {
		log.Fatalf("error closing reader: %v", err)
	}

	log.Infof("Search took %v", timeEnd.Sub(timeStart))
}

func create(bktree *bktree.BKTree, config settings.Config) {
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_division.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_segment.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_water.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_poi.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_infra.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_address.parquet"))
	processParquet(bktree, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_zipcode.parquet"))
}

func processParquet(bktree *bktree.BKTree, path string) {
	log.Infof("Inserting %s\n", path)

	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(Record), 4)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	batchSize := 10000
	sem := make(chan struct{}, 1) // Limit to 10 concurrent goroutines

	var wg sync.WaitGroup
	for {
		records := make([]Record, batchSize)
		var num int
		if err := pr.Read(&records); err != nil {
			log.Fatalf("Failed to read records: %v", err)
		}
		num = len(records)
		if num == 0 {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // Block if there are already 10 goroutines
		go func(records []Record) {
			defer wg.Done()
			defer func() { <-sem }() // Release a slot in the semaphore

			//batch := index.NewBatch()

			for _, rec := range records {
				if rec.Name == "" {
					continue
				}
				id := getNextID()

				// Process aliases
				processAliases(bktree, rec, id)
			}

			//insertDocs(writer, batch)
		}(records[:num])
	}

	wg.Wait()
	log.Infof("Inserted %s\n", path)
}

func processAliases(bktree *bktree.BKTree, rec Record, id uint64) {
	//batch = addAlias(batch, rec, rec.Name, id)

	// Add aliases for name aliases
	/* for name, alias := range aliases {
		if rec.Name == name {
			batch = addAlias(batch, rec, alias, id)
		}
	} */

	// Add embedding for truncated names
	for _, truncation := range truncations {
		if strings.Contains(rec.Name, truncation) {
			//alias := strings.Trim(strings.Replace(rec.Name, truncation, "", 1), " ")
			//batch = addAlias(batch, rec, alias, id)
		}
	}

	// Add alias for name + relation
	if len(rec.Relation) > 0 {
		relations := strings.Split(rec.Relation, ";")
		for _, relation := range relations {
			if rec.Name == relation {
				continue
			}

			//alias := rec.Name + " " + relation
			//batch = addAlias(batch, rec, alias, id)

			// Add entry for relation aliases
			/* for name, alias := range aliases {
				if relation == name {
					aliasEmbedding := rec.Name + " " + alias
					docAliases = addAlias(docAliases, rec, aliasEmbedding, id)
				}
			} */
		}
	}

	alias := preprocess(fmt.Sprintf("%s %s", rec.Name, rec.Relation))
	err := bktree.Insert(alias)
	if err != nil {
		log.Fatalf("error inserting: %v", err)
	}

	//alias := strings.ToLower(rec.Name)
	//wordCount := len(strings.Split(alias, " "))
	//classRank := getClassRank(rec.Class)
	//subclassRank := getSubclassScore(rec.Subclass)
	//charCount := len(strings.Replace(alias, " ", "", -1))

	//doc := bluge.NewDocument(fmt.Sprintf("%d", id))
	//doc.AddField(bluge.NewTextField("alias", alias).StoreValue())
	//doc.AddField(bluge.NewTextField("relation", rec.Relation).StoreValue())

	//doc.AddField(bluge.NewTextField("class", rec.Class).StoreValue())
	//doc.AddField(bluge.NewTextField("subclass", rec.Subclass).StoreValue())
	//doc.AddField(bluge.NewNumericField("classRank", float64(classRank)).Sortable())
	//doc.AddField(bluge.NewNumericField("subclassRank", float64(subclassRank)).Sortable())
	//doc.AddField(bluge.NewNumericField("wordCount", float64(wordCount)).Sortable())
	//doc.AddField(bluge.NewNumericField("charCount", float64(charCount)).Sortable())

	//batch.Insert(doc)

	//return batch
}

func addAlias(batch *index.Batch, rec Record, alias string, id uint64) *index.Batch {
	alias = strings.ToLower(alias)
	wordCount := len(strings.Split(alias, " "))
	classRank := getClassRank(rec.Class)
	subclassRank := getSubclassScore(rec.Subclass)
	charCount := len(strings.Replace(alias, " ", "", -1))

	doc := bluge.NewDocument(fmt.Sprintf("%d", id))
	doc.AddField(bluge.NewTextField("alias", alias).StoreValue())

	//doc.AddField(bluge.NewTextField("class", rec.Class).StoreValue())
	//doc.AddField(bluge.NewTextField("subclass", rec.Subclass).StoreValue())
	doc.AddField(bluge.NewNumericField("classRank", float64(classRank)).Sortable())
	doc.AddField(bluge.NewNumericField("subclassRank", float64(subclassRank)).Sortable())
	doc.AddField(bluge.NewNumericField("wordCount", float64(wordCount)).Sortable())
	doc.AddField(bluge.NewNumericField("charCount", float64(charCount)).Sortable())

	batch.Insert(doc)

	return batch
}

func insertDocs(writer *bluge.Writer, batch *index.Batch) {
	err := writer.Batch(batch)
	if err != nil {
		log.Fatalf("error executing batch: %v", err)
	}
}

func getClassRank(class string) int {
	switch class {
	case "division":
		return 1
	case "water": // lot of division names with partly water name, maas, ijssel, etc, rank the same
		return 1
	case "road":
		return 2
	case "infra":
		return 3
	case "address":
		return 4
	case "zipcode":
		return 5
	case "poi":
		return 6
	default:
		return 100
	}
}

func getSubclassScore(subclass string) int {
	switch subclass {
	case "locality":
		return 1
	case "county":
		return 2
	case "neighboorhood":
		return 3
	case "microhood":
		return 4
	case "motorway":
		return 1
	case "trunk":
		return 2
	case "primary":
		return 3
	case "secondary":
		return 4
	case "tertiary":
		return 5
	case "unclassified":
		return 6
	case "residential":
		return 6
	case "living_street":
		return 6
	default:
		return 100
	}
}
