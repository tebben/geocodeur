package handlers

import (
	"context"

	"github.com/tebben/geocodeur/service"
	"github.com/tebben/geocodeur/settings"
)

type LookupInput struct {
	ID uint64 `required:"true" json:"limit" path:"id" doc:"Maximum number of results to return" minimum:"0" example:"40231"`
}

type LookupResult struct {
	Body struct {
		Feature service.LookupResult `json:"feature"`
	}
}

func LookupHandler(config settings.Config) func(ctx context.Context, input *struct {
	LookupInput
}) (*LookupResult, error) {
	return func(ctx context.Context, input *struct {
		LookupInput
	}) (*LookupResult, error) {
		return nil, nil
		/* result, err := service.Lookup(config.Database.ConnectionString, input.ID)
		if err != nil {
			return nil, huma.Error400BadRequest(fmt.Sprintf("%v", err))
		}

		lookupResult := &LookupResult{}
		lookupResult.Body.Feature = result

		return lookupResult, nil */
	}
}
