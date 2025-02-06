package meili

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/tebben/geocodeur/httpclientpool"
	"github.com/tebben/geocodeur/settings"
)

func Request(query string) ([]byte, error) {
	settings := settings.GetConfig()
	client := httpclientpool.GetPoolInstance().GetClient()
	url := fmt.Sprintf("%s/indexes/geocodeur/search", settings.Meili.Host)
	requestBody := []byte(query)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", settings.Meili.Key))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var content []byte

	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()

		content, err = io.ReadAll(gzipReader)
		if err != nil {
			return nil, err
		}
	} else {
		content, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	}

	return content, nil
}

func SetPrefixSearch() error {
	settings := settings.GetConfig()
	client := httpclientpool.GetPoolInstance().GetClient()
	url := fmt.Sprintf("%s/indexes/geocodeur/settings/prefix-search", settings.Meili.Host)

	body := strings.NewReader(`"disabled"`)

	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", settings.Meili.Key))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error setting prefix-search: %v", err)
	}
	defer resp.Body.Close()

	return nil
}

/*
curl \
  -X PUT 'http://localhost:7700/indexes/geocodeur/settings/prefix-search' \
  -H "Authorization: Bearer E8H-DDQUGhZhFWhTq263Ohd80UErhFmLIFnlQK81oeQ" \
  -H 'Content-Type: application/json' \
  --data-binary '"indexingTime"'


curl \
  -X GET 'http://localhost:7700/indexes/geocodeur/settings/prefix-search' \
  -H "Authorization: Bearer E8H-DDQUGhZhFWhTq263Ohd80UErhFmLIFnlQK81oeQ"
*/
