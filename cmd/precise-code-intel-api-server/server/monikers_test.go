package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestLookupMoniker(t *testing.T) {
	dump := db.Dump{
		ID:   50,
		Root: "sub/",
	}

	moniker := bundles.MonikerData{
		Kind:                 "import",
		Scheme:               "gomod",
		Identifier:           "pad",
		PackageInformationID: "1234",
	}

	r1 := bundles.Range{
		Start: bundles.Position{Line: 10, Character: 50},
		End:   bundles.Position{Line: 10, Character: 55},
	}
	r2 := bundles.Range{
		Start: bundles.Position{Line: 11, Character: 50},
		End:   bundles.Position{Line: 11, Character: 55},
	}
	r3 := bundles.Range{
		Start: bundles.Position{Line: 12, Character: 50},
		End:   bundles.Position{Line: 12, Character: 55},
	}
	r4 := bundles.Range{
		Start: bundles.Position{Line: 13, Character: 50},
		End:   bundles.Position{Line: 13, Character: 55},
	}
	r5 := bundles.Range{
		Start: bundles.Position{Line: 14, Character: 50},
		End:   bundles.Position{Line: 14, Character: 55},
	}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient1 := &mockBundleClient{}
	mockBundleClient2 := &mockBundleClient{}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		switch bundleID {
		case 42:
			return mockBundleClient1
		case 50:
			return mockBundleClient2
		}

		t.Fatalf("unexpected bundle id %d", bundleID)
		return nil
	}

	// resolves package information from moniker
	mockBundleClient1.packageInformation = func(ctx context.Context, path, packageInformationID string) (bundles.PackageInformationData, error) {
		if path != "sub/main.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if packageInformationID != "1234" {
			t.Errorf("unexpected package information id. want=%v have=%v", "1234", packageInformationID)
		}
		return bundles.PackageInformationData{Name: "leftpad", Version: "0.1.0"}, nil
	}

	// returns dump that provides package
	mockDB.getPackage = func(ctx context.Context, scheme, name, version string) (db.Dump, bool, error) {
		if scheme != "gomod" {
			t.Errorf("unexpected scheme. want=%v have=%v", "gomod", scheme)
		}
		if name != "leftpad" {
			t.Errorf("unexpected name. want=%v have=%v", "leftpad", name)
		}
		if version != "0.1.0" {
			t.Errorf("unexpected version. want=%v have=%v", "0.1.0", version)
		}
		return dump, true, nil
	}

	// returns monikers from package dump
	mockBundleClient2.monikerResults = func(ctx context.Context, modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
		if modelType != "definitions" {
			t.Errorf("unexpected model type. want=%v have=%v", "definitions", modelType)
		}
		if scheme != "gomod" {
			t.Errorf("unexpected scheme. want=%v have=%v", "gomod", scheme)
		}
		if identifier != "pad" {
			t.Errorf("unexpected identifier. want=%v have=%v", "pad", identifier)
		}
		if skip != 10 {
			t.Errorf("unexpected skip. want=%d have=%d", 10, skip)
		}
		if take != 5 {
			t.Errorf("unexpected take. want=%d have=%d", 5, take)
		}
		return []bundles.Location{
			{DumpID: 42, Path: "foo.go", Range: r1},
			{DumpID: 42, Path: "bar.go", Range: r2},
			{DumpID: 42, Path: "baz.go", Range: r3},
			{DumpID: 42, Path: "bar.go", Range: r4},
			{DumpID: 42, Path: "baz.go", Range: r5},
		}, 15, nil
	}

	locations, totalCount, err := lookupMoniker(mockDB, mockBundleManagerClient, 42, "sub/main.go", "definitions", moniker, 10, 5)
	if err != nil {
		t.Fatalf("unexpected error querying moniker: %s", err)
	}
	if totalCount != 15 {
		t.Errorf("unexpected total count. want=%v have=%v", 5, totalCount)
	}

	expectedLocations := []ResolvedLocation{
		{Dump: dump, Path: "sub/foo.go", Range: r1},
		{Dump: dump, Path: "sub/bar.go", Range: r2},
		{Dump: dump, Path: "sub/baz.go", Range: r3},
		{Dump: dump, Path: "sub/bar.go", Range: r4},
		{Dump: dump, Path: "sub/baz.go", Range: r5},
	}
	if !reflect.DeepEqual(locations, expectedLocations) {
		t.Errorf("unexpected definitions. want=%v have=%v", expectedLocations, locations)
	}
}

func TestLookupMonikerNoPackageInformationID(t *testing.T) {
	moniker := bundles.MonikerData{
		Kind:                 "import",
		Scheme:               "gomod",
		Identifier:           "pad",
		PackageInformationID: "",
	}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}

	_, totalCount, err := lookupMoniker(mockDB, mockBundleManagerClient, 42, "sub/main.go", "definitions", moniker, 10, 5)
	if err != nil {
		t.Fatalf("unexpected error querying moniker: %s", err)
	}
	if totalCount != 0 {
		t.Errorf("unexpected total count. want=%v have=%v", 0, totalCount)
	}
}

func TestLookupMonikerNoPackage(t *testing.T) {
	moniker := bundles.MonikerData{
		Kind:                 "import",
		Scheme:               "gomod",
		Identifier:           "pad",
		PackageInformationID: "1234",
	}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient := &mockBundleClient{}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		return mockBundleClient
	}
	mockBundleClient.packageInformation = func(ctx context.Context, path, packageInformationID string) (bundles.PackageInformationData, error) {
		return bundles.PackageInformationData{Name: "leftpad", Version: "0.1.0"}, nil
	}
	mockDB.getPackage = func(ctx context.Context, scheme, name, version string) (db.Dump, bool, error) {
		return db.Dump{}, false, nil
	}

	_, totalCount, err := lookupMoniker(mockDB, mockBundleManagerClient, 42, "sub/main.go", "definitions", moniker, 10, 5)
	if err != nil {
		t.Fatalf("unexpected error querying moniker: %s", err)
	}
	if totalCount != 0 {
		t.Errorf("unexpected total count. want=%v have=%v", 0, totalCount)
	}
}
