package server

import (
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

type ResolvedLocation struct {
	Dump  db.Dump       `json:"dump"`
	Path  string        `json:"path"`
	Range bundles.Range `json:"range"`
}

type APILocation struct {
	RepositoryID int           `json:"repositoryId"`
	Commit       string        `json:"commit"`
	Path         string        `json:"path"`
	Range        bundles.Range `json:"range"`
}

func resolveLocationsWithDump(dump db.Dump, locations []bundles.Location) []ResolvedLocation {
	var resolvedLocations []ResolvedLocation
	for _, location := range locations {
		resolvedLocations = append(resolvedLocations, ResolvedLocation{
			Dump:  dump,
			Path:  dump.Root + location.Path,
			Range: location.Range,
		})
	}

	return resolvedLocations
}

func resolveLocations(db *db.DB, locations []bundles.Location) ([]ResolvedLocation, error) {
	dumpsByID, err := db.GetDumps(dumpIDs(locations))
	if err != nil {
		return nil, err
	}

	var resolvedLocations []ResolvedLocation
	for _, location := range locations {
		dump := dumpsByID[location.DumpID]

		resolvedLocations = append(resolvedLocations, ResolvedLocation{
			Dump:  dump,
			Path:  dump.Root + location.Path,
			Range: location.Range,
		})
	}

	return resolvedLocations, nil
}

func dumpIDs(locations []bundles.Location) []int {
	uniqueIDs := map[int]struct{}{}
	for _, l := range locations {
		uniqueIDs[l.DumpID] = struct{}{}
	}

	var ids []int
	for k := range uniqueIDs {
		ids = append(ids, k)
	}

	return ids
}
func serializeLocations(resolvedLocations []ResolvedLocation) ([]APILocation, error) {
	var apiLocations []APILocation
	for _, res := range resolvedLocations {
		apiLocations = append(apiLocations, APILocation{
			RepositoryID: res.Dump.RepositoryID,
			Commit:       res.Dump.Commit,
			Path:         res.Path,
			Range:        res.Range,
		})
	}

	return apiLocations, nil
}
