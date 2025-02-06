package meili

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/meilisearch/meilisearch-go"
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
	client := meiliGetClient(config.Meili)
	index := meiliGetIndex(client)
	meiliSetPrefixSearch()
	meailiCreate(index, config)
}

func meiliGetClient(config settings.MeiliConfig) meilisearch.ServiceManager {
	client := meilisearch.New(config.Host, meilisearch.WithAPIKey(config.Key))

	return client
}

func meiliSetPrefixSearch() {
	SetPrefixSearch()
}

func meiliGetIndex(client meilisearch.ServiceManager) meilisearch.IndexManager {
	index := client.Index("geocodeur")
	distinctAttribute := "id"
	settings := meilisearch.Settings{
		SearchableAttributes: []string{"alias"},
		FilterableAttributes: []string{"class", "subclass"},
		RankingRules:         []string{"words", "proximity", "classRank:asc", "wordCount:asc", "exactness", "subclassRank:asc", "typo", "sort"},
		DistinctAttribute:    &distinctAttribute,
		Synonyms: map[string][]string{
			"den bosch": []string{"'s-hertogenbosch"},
		},
		ProximityPrecision: "byWord",
		SeparatorTokens:    []string{},
		NonSeparatorTokens: []string{"-", "_", "\"", ":", "/", "\\", "@", "+", "~", "=", "^", "*", "#", ".", ";", ",", "!", "?", "(", ")", "[", "]", "{", "}", "|"},
		StopWords:          []string{},
	}
	_, err := index.UpdateSettings(&settings)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return index
}

func meailiCreate(index meilisearch.IndexManager, config settings.Config) {
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_division.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_segment.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_water.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_poi.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_infra.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_address.parquet"))
	processParquet(index, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_zipcode.parquet"))
}

func processParquet(index meilisearch.IndexManager, path string) {
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

	batchSize := 1000
	sem := make(chan struct{}, 20) // Limit to 20 concurrent goroutines

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

			documents := []map[string]interface{}{}

			for _, rec := range records {
				if rec.Name == "" {
					continue
				}
				id := getNextID()

				// Process aliases
				documents = processAliases(documents, rec, id)
			}

			insertDocs(index, documents)
		}(records[:num])
	}

	wg.Wait()
	log.Infof("Inserted %s\n", path)
}

func processAliases(documents []map[string]interface{}, rec Record, id uint64) []map[string]interface{} {

	//documents = addAlias(documents, rec, rec.Name, id)
	//var docAliases []string
	documents = addAlias(documents, rec, rec.Name, id)

	// Add aliases for name aliases
	/* for name, alias := range aliases {
		if rec.Name == name {
			documents = addAlias(documents, rec, alias, id)
		}
	} */

	// Add embedding for truncated names
	for _, truncation := range truncations {
		if strings.Contains(rec.Name, truncation) {
			alias := strings.Trim(strings.Replace(rec.Name, truncation, "", 1), " ")
			documents = addAlias(documents, rec, alias, id)
		}
	}

	// Add alias for name + relation
	if len(rec.Relation) > 0 {
		relations := strings.Split(rec.Relation, ";")
		for _, relation := range relations {
			if rec.Name == relation {
				continue
			}

			alias := rec.Name + " " + relation
			documents = addAlias(documents, rec, alias, id)

			// Add entry for relation aliases
			/* for name, alias := range aliases {
				if relation == name {
					aliasEmbedding := rec.Name + " " + alias
					docAliases = addAlias(docAliases, rec, aliasEmbedding, id)
				}
			} */
		}
	}

	/* classRank := getClassRank(rec.Class)
	subclassRank := getSubclassScore(rec.Subclass)
	wordCount := len(strings.Split(rec.Name, " "))
	//aliasCount := len(docAliases)
	centroid := strings.Split(strings.Replace(strings.Replace(rec.Centroid, "POINT (", "", 1), ")", "", 1), " ")

	relations := strings.Split(rec.Relation, ";")
	for i, relation := range relations {
		relations[i] = strings.ToLower(strings.Trim(relation, " "))
	} */

	/* documents = append(documents, map[string]interface{}{
		"id":           id,
		"name":         rec.Name,
		"relations":    relations,
		"class":        rec.Class,
		"classRank":    classRank,
		"subclass":     rec.Subclass,
		"subclassRank": subclassRank,
		"aliases":      docAliases,
		//"aliasCount":   aliasCount,
		"wordCount": wordCount,
		"_geo": map[string]interface{}{
			"lng": centroid[0],
			"lat": centroid[1],
		},
	}) */

	return documents
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
		return 7
	case "living_street":
		return 8
	default:
		return 100
	}
}

func addAlias(documents []map[string]interface{}, rec Record, alias string, id uint64) []map[string]interface{} {
	alias = strings.ToLower(alias)

	wordCount := len(strings.Split(alias, " "))

	classRank := getClassRank(rec.Class)
	subclassRank := getSubclassScore(rec.Subclass)

	//docAliases = append(docAliases, alias)
	documents = append(documents, map[string]interface{}{
		"id":           id,
		"alias":        alias,
		"class":        rec.Class,
		"classRank":    classRank,
		"subclass":     rec.Subclass,
		"subclassRank": subclassRank,
		"wordCount":    wordCount,
	})

	//docAliases = append(docAliases, alias)

	return documents
}

func insertDocs(index meilisearch.IndexManager, documents []map[string]interface{}) {
	_, err := index.AddDocuments(documents)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
