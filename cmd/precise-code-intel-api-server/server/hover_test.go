package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestHover(t *testing.T) {
	dump := db.Dump{
		ID:   42,
		Root: "sub/",
	}

	expectedRange := bundles.Range{
		Start: bundles.Position{Line: 10, Character: 50},
		End:   bundles.Position{Line: 10, Character: 55},
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

	// returns hover text from same dump
	mockBundleClient.hover = func(path string, line, character int) (text string, r bundles.Range, exists bool, err error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if line != 10 {
			t.Errorf("unexpected line. want=%d have=%d", 10, line)
		}
		if character != 50 {
			t.Errorf("unexpected character. want=%d have=%d", 50, character)
		}
		return "text", expectedRange, true, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	text, r, exists, err := s.hover("sub/main.go", 10, 50, 42)
	if err != nil {
		t.Fatalf("expected error getting hover text: %s", err)
	}
	if !exists {
		t.Fatalf("expected hover text to exist.")
	}

	if text != "text" {
		t.Errorf("unexpected text. want=%v have=%v", "text", text)
	}
	if !reflect.DeepEqual(r, expectedRange) {
		t.Errorf("unexpected range. want=%v have=%v", expectedRange, r)
	}
}

func TestHoverUnknownDump(t *testing.T) {
	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return db.Dump{}, false, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	_, _, _, err := s.hover("sub/main.go", 10, 50, 42)
	if err != ErrMissingDump {
		t.Errorf("unexpected error getting hover text. want=%v have=%v", ErrMissingDump, err)
	}
}

func TestHoverRemoteDefinitionHoverText(t *testing.T) {
	dump1 := db.Dump{ID: 42, Root: "sub1/"}
	dump2 := db.Dump{ID: 50, Root: "sub2/"}

	r1 := bundles.Range{
		Start: bundles.Position{Line: 20, Character: 13},
		End:   bundles.Position{Line: 20, Character: 15},
	}
	r2 := bundles.Range{
		Start: bundles.Position{Line: 21, Character: 50},
		End:   bundles.Position{Line: 21, Character: 55},
	}
	r3 := bundles.Range{
		Start: bundles.Position{Line: 22, Character: 50},
		End:   bundles.Position{Line: 22, Character: 55},
	}

	expectedRange := bundles.Range{
		Start: bundles.Position{Line: 10, Character: 50},
		End:   bundles.Position{Line: 10, Character: 55},
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

	// returns no hover text from same dump
	mockBundleClient1.hover = func(path string, line, character int) (text string, r bundles.Range, exists bool, err error) {
		return "", bundles.Range{}, false, nil
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
		return bundles.PackageInformationData{Name: "leftpad", Version: "0.1.0"}, nil
	}
	// returns dump that provides package
	mockDB.getPackage = func(ctx context.Context, scheme, name, version string) (db.Dump, bool, error) {
		return dump2, true, nil
	}
	// returns monikers from remote dump
	mockBundleClient2.monikerResults = func(modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
		return []bundles.Location{
			{DumpID: 50, Path: "foo.go", Range: r1},
			{DumpID: 50, Path: "bar.go", Range: r2},
			{DumpID: 50, Path: "baz.go", Range: r3},
		}, 15, nil
	}

	// returns hover text from remote dump
	mockBundleClient2.hover = func(path string, line, character int) (text string, r bundles.Range, exists bool, err error) {
		if path != "foo.go" {
			t.Errorf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if line != 20 {
			t.Errorf("unexpected line. want=%d have=%d", 20, line)
		}
		if character != 13 {
			t.Errorf("unexpected character. want=%d have=%d", 13, character)
		}
		return "text", expectedRange, true, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	text, r, exists, err := s.hover("sub1/main.go", 10, 50, 42)
	if err != nil {
		t.Fatalf("expected error getting hover text: %s", err)
	}
	if !exists {
		t.Fatalf("expected hover text to exist.")
	}

	if text != "text" {
		t.Errorf("unexpected text. want=%v have=%v", "text", text)
	}
	if !reflect.DeepEqual(r, expectedRange) {
		t.Errorf("unexpected range. want=%v have=%v", expectedRange, r)
	}
}

func TestHoverUnknownDefinition(t *testing.T) {
	dump := db.Dump{
		ID:   42,
		Root: "sub1/",
	}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient := &mockBundleClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return dump, true, nil
	}
	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		return mockBundleClient
	}

	// returns no hover text from same dump
	mockBundleClient.hover = func(path string, line, character int) (text string, r bundles.Range, exists bool, err error) {
		return "", bundles.Range{}, false, nil
	}

	// returns no local definitions
	mockBundleClient.definitions = func(path string, line, character int) ([]bundles.Location, error) {
		return nil, nil
	}

	// returns monikers attached to range
	mockBundleClient.monikersByPosition = func(path string, line, character int) ([][]bundles.MonikerData, error) {
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
	mockBundleClient.packageInformation = func(path, packageInformationID string) (bundles.PackageInformationData, error) {
		return bundles.PackageInformationData{Name: "leftpad", Version: "0.1.0"}, nil
	}
	// no dump provides package
	mockDB.getPackage = func(ctx context.Context, scheme, name, version string) (db.Dump, bool, error) {
		return db.Dump{}, false, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	_, _, exists, err := s.hover("sub/main.go", 10, 50, 42)
	if err != nil {
		t.Errorf("unexpected error getting hover text: %s", err)
	}
	if exists {
		t.Errorf("unexpected hover text")
	}
}
