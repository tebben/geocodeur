package service

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/meili"
)

type GeocodeResult struct {
	ID         uint64          `json:"id,omitempty" doc:"The id of the feature, not the original Overture id"`
	Name       string          `json:"name,omitempty" doc:"The name of the feature"`
	Class      string          `json:"class,omitempty" doc:"The class of the feature"`
	Subclass   string          `json:"subclass,omitempty" doc:"The subclass of the feature"`
	Relations  []string        `json:"relations,omitempty" doc:"The relations of the feature"`
	Similarity float64         `json:"similarity,omitempty" doc:"The similarity score q <-> alias, the higher the better"`
	Geom       json.RawMessage `json:"geom,omitempty" doc:"The geometry of the feature in GeoJSON format"`
}

type Class string

const (
	Division Class = "division"
	Road     Class = "road"
	Water    Class = "water"
	Poi      Class = "poi"
	Infra    Class = "infra"
	Address  Class = "address"
	Zipcode  Class = "zipcode"
)

func StringToClass(s string) (Class, error) {
	switch s {
	case string(Division):
		return Division, nil
	case string(Road):
		return Road, nil
	case string(Water):
		return Water, nil
	case string(Poi):
		return Poi, nil
	case string(Infra):
		return Infra, nil
	case string(Address):
		return Address, nil
	case string(Zipcode):
		return Zipcode, nil
	default:
		return "", fmt.Errorf("class %s not found", s)
	}
}

type GeocodeOptions struct {
	Limit           uint16
	Classes         []Class
	IncludeGeometry bool
}

func (g GeocodeOptions) ClassesToStringArray() []string {
	stringClasses := make([]string, len(g.Classes))
	for i, class := range g.Classes {
		stringClasses[i] = strings.ToLower(string(class))
	}

	return stringClasses
}

func (g GeocodeOptions) ClassesToSqlArray() string {
	classes := g.Classes
	if classes == nil || len(classes) == 0 {
		classes = []Class{Division, Road, Water, Poi, Infra, Address, Zipcode}
	}

	lowerClasses := make([]string, len(classes))
	for i, class := range classes {
		lowerClasses[i] = fmt.Sprintf("'%s'", strings.ToLower(string(class)))
	}

	return fmt.Sprintf("(%s)", strings.Join(lowerClasses, ", "))
}

// new GeocodeOptions with default values
func NewGeocodeOptions(limit uint16, classes []Class, includeGeom bool) GeocodeOptions {
	return GeocodeOptions{
		Limit:           limit,
		Classes:         classes,
		IncludeGeometry: includeGeom,
	}
}

func Geocode(options GeocodeOptions, input string) ([]GeocodeResult, error) {
	input = strings.ToLower(input)
	query := createGeocodeQuery(options, input)
	data, err := meili.Request(query)
	if err != nil {
		return nil, err
	}

	geocodeResults, err := parseGeocodeResults(data)
	if err != nil {
		return nil, err
	}

	return geocodeResults, nil
}

func createGeocodeQuery(options GeocodeOptions, input string) string {
	attributesToRetrieve := []string{"id", "name", "class", "subclass"}
	if options.IncludeGeometry {
		attributesToRetrieve = append(attributesToRetrieve, "geom")
	}

	quotedAttributes := strings.Join(quoteStrings(attributesToRetrieve), ",")

	filter := ""
	if len(options.Classes) > 0 {
		filter = fmt.Sprintf(`, "filter": "class IN [%v]"`, strings.ReplaceAll(
			strings.Join(quoteStrings(options.ClassesToStringArray()), ","),
			"\"", "\\\""))
	}

	query := fmt.Sprintf(`{
		"q": "%s",
		"attributesToRetrieve": [%v],
		"limit": %v,
		"showRankingScore": true,
		"showRankingScoreDetails": true
		%s
	}`, input, quotedAttributes, options.Limit, filter)

	return query
}

/* func parseGeocodeResults(data []byte) ([]GeocodeResult, error) {
	jsonString := string(data)
	geocodeResults := []GeocodeResult{}

	var result map[string]interface{}
	json.Unmarshal([]byte(jsonString), &result)
	hits := result["hits"].([]interface{})

	log.Infof("processing time: %v ms", result["processingTimeMs"])

	for _, hit := range hits {
		hitMap := hit.(map[string]interface{})
		id := hitMap["id"].(float64)
		name := hitMap["name"].(string)
		similarity := math.Round(hitMap["_rankingScore"].(float64)*1000) / 1000

		relations := make([]string, len(hitMap["relations"].([]interface{})))
		for i, relation := range hitMap["relations"].([]interface{}) {
			relations[i] = relation.(string)
		}
		class := hitMap["class"].(string)
		subclass := hitMap["subclass"].(string)
		geom := ""
		if g, ok := hitMap["geom"].(string); ok {
			geom = g
		}

		geocodeResult := GeocodeResult{ID: uint64(id), Name: name, Class: class, Subclass: subclass, Relations: relations, Similarity: similarity, Geom: json.RawMessage(geom)}
		geocodeResults = append(geocodeResults, geocodeResult)
	}

	geocodeResults = orderGeocodeResults(geocodeResults)
	return geocodeResults, nil
} */

type GeocodeAPIResponse struct {
	Hits             []GeocodeHit `json:"hits"`
	ProcessingTimeMs int          `json:"processingTimeMs"`
}

type GeocodeHit struct {
	ID           uint64          `json:"id"`
	Name         string          `json:"name"`
	Class        string          `json:"class"`
	Subclass     string          `json:"subclass"`
	Relations    []string        `json:"relations"`
	RankingScore float64         `json:"_rankingScore"`
	Geom         json.RawMessage `json:"geom"`
}

func parseGeocodeResults(data []byte) ([]GeocodeResult, error) {
	var response GeocodeAPIResponse
	err := json.Unmarshal(data, &response)
	log.Info(string(data))

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal geocode results: %w", err)
	}

	log.Infof("processing time: %d ms", response.ProcessingTimeMs)

	// Map hits to GeocodeResult
	geocodeResults := make([]GeocodeResult, len(response.Hits))
	for i, hit := range response.Hits {
		geocodeResults[i] = GeocodeResult{
			ID:         hit.ID,
			Name:       hit.Name,
			Class:      hit.Class,
			Subclass:   hit.Subclass,
			Relations:  hit.Relations,
			Similarity: math.Round(hit.RankingScore*1000) / 1000,
			Geom:       hit.Geom,
		}
	}

	//geocodeResults = orderGeocodeResults(geocodeResults)
	return geocodeResults, nil
}

func getClassScore(class string) int {
	switch class {
	case "division":
		return 1
	case "water":
		return 2
	case "road":
		return 3
	case "infra":
		return 4
	case "address":
		return 5
	case "zipcode":
		return 6
	case "poi":
		return 7
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

func orderGeocodeResults(results []GeocodeResult) []GeocodeResult {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Similarity != results[j].Similarity {
			return results[i].Similarity > results[j].Similarity
		}
		classScoreI := getClassScore(results[i].Class)
		classScoreJ := getClassScore(results[j].Class)
		if classScoreI != classScoreJ {
			return classScoreI < classScoreJ
		}
		subclassScoreI := getSubclassScore(results[i].Subclass)
		subclassScoreJ := getSubclassScore(results[j].Subclass)
		return subclassScoreI < subclassScoreJ
	})
	return results
}

func quoteStrings(slice []string) []string {
	for i, s := range slice {
		slice[i] = fmt.Sprintf(`"%s"`, s)
	}
	return slice
}
