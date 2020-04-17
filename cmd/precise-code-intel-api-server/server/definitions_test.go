package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestDefinitions(t *testing.T) {
	dump := db.Dump{
		ID:   42,
		Root: "sub/",
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

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient := &mockBundleClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return dump, true, nil
	}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		if bundleID != 42 {
			t.Errorf("unexpected bundleID. want=%v have=%v", 42, bundleID)
		}
		return mockBundleClient
	}

	// returns local definitions
	mockBundleClient.definitions = func(path string, line, character int) ([]bundles.Location, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if line != 10 {
			t.Errorf("unexpected line. want=%d have=%d", 10, line)
		}
		if character != 50 {
			t.Errorf("unexpected character. want=%d have=%d", 50, character)
		}
		return []bundles.Location{
			{DumpID: 42, Path: "foo.go", Range: r1},
			{DumpID: 42, Path: "bar.go", Range: r2},
			{DumpID: 42, Path: "baz.go", Range: r3},
		}, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	definitions, err := s.definitions("sub/main.go", 10, 50, 42)
	if err != nil {
		t.Fatalf("expected error getting hover text: %s", err)
	}

	expectedDefinitions := []ResolvedLocation{
		{Dump: dump, Path: "sub/foo.go", Range: r1},
		{Dump: dump, Path: "sub/bar.go", Range: r2},
		{Dump: dump, Path: "sub/baz.go", Range: r3},
	}
	if !reflect.DeepEqual(definitions, expectedDefinitions) {
		t.Errorf("unexpected definitions. want=%v have=%v", expectedDefinitions, definitions)
	}
}

func TestDefinitionsUnknownDump(t *testing.T) {
	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return db.Dump{}, false, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	_, err := s.definitions("sub/main.go", 10, 50, 25)
	if err != ErrMissingDump {
		t.Errorf("unexpected error getting hover text. want=%v have=%v", ErrMissingDump, err)
	}
}

func TestDefinitionViaSameDumpMoniker(t *testing.T) {
	dump := db.Dump{
		ID:   42,
		Root: "sub/",
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

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient := &mockBundleClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return dump, true, nil
	}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		if bundleID != 42 {
			t.Errorf("unexpected bundleID. want=%v have=%v", 42, bundleID)
		}
		return mockBundleClient
	}

	// returns no local definitions
	mockBundleClient.definitions = func(path string, line, character int) ([]bundles.Location, error) {
		return nil, nil
	}

	// returns monikers attached to range
	mockBundleClient.monikersByPosition = func(path string, line, character int) ([][]bundles.MonikerData, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if line != 10 {
			t.Errorf("unexpected line. want=%d have=%d", 10, line)
		}
		if character != 50 {
			t.Errorf("unexpected character. want=%d have=%d", 50, character)
		}
		return [][]bundles.MonikerData{
			{
				bundles.MonikerData{
					Kind:       "export",
					Scheme:     "gomod",
					Identifier: "pad",
				},
			},
		}, nil
	}

	// returns locations of export moniker from same dump
	mockBundleClient.monikerResults = func(modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
		if modelType != "definitions" {
			t.Errorf("unexpected model type. want=%v have=%v", "definitions", modelType)
		}
		if scheme != "gomod" {
			t.Errorf("unexpected scheme. want=%v have=%v", "gomod", scheme)
		}
		if identifier != "pad" {
			t.Errorf("unexpected identifier. want=%v have=%v", "pad", identifier)
		}
		if skip != 0 {
			t.Errorf("unexpected skip. want=%d have=%d", 0, skip)
		}
		if take != 0 {
			t.Errorf("unexpected take. want=%d have=%d", 0, take)
		}
		return []bundles.Location{
			{DumpID: 42, Path: "foo.go", Range: r1},
			{DumpID: 42, Path: "bar.go", Range: r2},
			{DumpID: 42, Path: "baz.go", Range: r3},
		}, 3, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	definitions, err := s.definitions("sub/main.go", 10, 50, 42)
	if err != nil {
		t.Fatalf("expected error getting hover text: %s", err)
	}

	expectedDefinitions := []ResolvedLocation{
		{Dump: dump, Path: "sub/foo.go", Range: r1},
		{Dump: dump, Path: "sub/bar.go", Range: r2},
		{Dump: dump, Path: "sub/baz.go", Range: r3},
	}
	if !reflect.DeepEqual(definitions, expectedDefinitions) {
		t.Errorf("unexpected definitions. want=%v have=%v", expectedDefinitions, definitions)
	}
}

func TestDefinitionViaRemoteDumpMoniker(t *testing.T) {
	dump1 := db.Dump{ID: 42, Root: "sub1/"}
	dump2 := db.Dump{ID: 50, Root: "sub2/"}

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

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient1 := &mockBundleClient{}
	mockBundleClient2 := &mockBundleClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		switch id {
		case 42:
			return dump1, true, nil
		case 50:
			return dump2, true, nil
		}

		return db.Dump{}, false, nil
	}

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

	// returns no local definitions
	mockBundleClient1.definitions = func(path string, line, character int) ([]bundles.Location, error) {
		return nil, nil
	}

	// returns monikers attached to range
	mockBundleClient1.monikersByPosition = func(path string, line, character int) ([][]bundles.MonikerData, error) {
		return [][]bundles.MonikerData{
			{
				bundles.MonikerData{
					Kind:                 "import",
					Scheme:               "gomod",
					Identifier:           "pad",
					PackageInformationID: "1234",
				},
			},
		}, nil
	}

	// resolves package information from moniker
	mockBundleClient1.packageInformation = func(path, packageInformationID string) (bundles.PackageInformationData, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if packageInformationID != "1234" {
			t.Errorf("unexpected package information ID. want=%v have=%v", "1234", packageInformationID)
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
		return dump2, true, nil
	}

	// returns monikers from remote dump
	mockBundleClient2.monikerResults = func(modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
		if modelType != "definitions" {
			t.Errorf("unexpected model type. want=%v have=%v", "definitions", modelType)
		}
		if scheme != "gomod" {
			t.Errorf("unexpected scheme. want=%v have=%v", "gomod", scheme)
		}
		if identifier != "pad" {
			t.Errorf("unexpected identifier. want=%v have=%v", "pad", identifier)
		}
		if skip != 0 {
			t.Errorf("unexpected skip. want=%d have=%d", 0, skip)
		}
		if take != 0 {
			t.Errorf("unexpected take. want=%d have=%d", 0, take)
		}
		return []bundles.Location{
			{DumpID: 50, Path: "foo.go", Range: r1},
			{DumpID: 50, Path: "bar.go", Range: r2},
			{DumpID: 50, Path: "baz.go", Range: r3},
		}, 15, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	definitions, err := s.definitions("sub1/main.go", 10, 50, 42)
	if err != nil {
		t.Fatalf("expected error getting hover text: %s", err)
	}

	expectedDefinitions := []ResolvedLocation{
		{Dump: dump2, Path: "sub2/foo.go", Range: r1},
		{Dump: dump2, Path: "sub2/bar.go", Range: r2},
		{Dump: dump2, Path: "sub2/baz.go", Range: r3},
	}
	if !reflect.DeepEqual(definitions, expectedDefinitions) {
		t.Errorf("unexpected definitions. want=%v have=%v", expectedDefinitions, definitions)
	}
}
