package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/tebben/geocodeur/errors"
	"github.com/tebben/geocodeur/service"
	"github.com/tebben/geocodeur/settings"
)

type GeocodeInput struct {
	Query string   `required:"true" json:"q" query:"q" doc:"The search term to find a feature, the geocoder handles incomplete names and falls back to fuzzy search for typing errors. This way things as 'kerkstr ams' and 'kerkst masterdam' can still be found" example:"President Kennedylaan Amsterdam"`
	Limit uint16   `required:"falase" json:"limit" query:"limit" doc:"Maximum number of results to return" minimum:"1" maximum:"100" default:"10"`
	Class []string `required:"false" json:"class" query:"class" doc:"Filter results by class, this is a comma separated list. Leave empty to query on all classes" default:"division,water,road,poi" example:"division,water,road,poi" uniqueItems:"true"`
	Geom  bool     `required:"false" json:"geom" query:"geom" doc:"Include the geometry of the feature in the result" default:"false"`
}

type GeocodeResult struct {
	Body struct {
		QueryTime float32                 `json:"queryTime" doc:"Time in milliseconds it took to execute the query internally"`
		Results   []service.GeocodeResult `json:"results"`
	}
}

func GeocodeHandler(config settings.Config) func(ctx context.Context, input *struct {
	GeocodeInput
}) (*GeocodeResult, error) {
	return func(ctx context.Context, input *struct {
		GeocodeInput
	}) (*GeocodeResult, error) {

		geocodeOptions, error := createGeocoderOptions(config, input.GeocodeInput)
		if error != nil {
			return nil, huma.Error400BadRequest(error.Error())
		}

		timeStart := time.Now()
		results, err := service.Geocode(config.Database.ConnectionString, geocodeOptions, input.Query)
		if err != nil {
			return nil, huma.Error400BadRequest(fmt.Sprintf("%v", err))
		}

		geocodeResult := &GeocodeResult{}
		geocodeResult.Body.QueryTime = float32(time.Now().Sub(timeStart).Milliseconds())
		geocodeResult.Body.Results = results

		return geocodeResult, nil
	}
}

func createGeocoderOptions(config settings.Config, input GeocodeInput) (service.GeocodeOptions, *errors.APIError) {
	classes, err := getClasses(input)
	if err != nil {
		return service.GeocodeOptions{}, errors.NewAPIError(http.StatusBadRequest, err.Error(), nil)
	}

	return service.NewGeocodeOptions(config.API.PGTRGMTreshold, input.Limit, classes, input.Geom), nil
}

func getClasses(input GeocodeInput) ([]service.Class, error) {
	var classes []service.Class = make([]service.Class, len(input.Class))

	for i, v := range input.Class {
		class, err := service.StringToClass(strings.ToLower(v))
		if err != nil {
			return nil, err
		}

		classes[i] = class
	}

	return classes, nil
}
