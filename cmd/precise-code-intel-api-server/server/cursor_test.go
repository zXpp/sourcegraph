package server

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestSerializationRoundTrip(t *testing.T) {
	c := Cursor{
		Phase:     "same-repo",
		DumpID:    42,
		Path:      "/foo/bar/baz.go",
		Line:      10,
		Character: 50,
		Monikers: []bundles.MonikerData{
			{Kind: "k1", Scheme: "s1", Identifier: "i1", PackageInformationID: "pid1"},
			{Kind: "k2", Scheme: "s2", Identifier: "i2", PackageInformationID: "pid2"},
			{Kind: "k3", Scheme: "s3", Identifier: "i3", PackageInformationID: "pid3"},
		},
		SkipResults:            1,
		Identifier:             "x",
		Scheme:                 "gomod",
		Name:                   "leftpad",
		Version:                "0.1.0",
		DumpIDs:                []int{1, 2, 3, 4, 5},
		TotalDumpsWhenBatching: 5,
		SkipDumpsWhenBatching:  4,
		SkipDumpsInBatch:       3,
		SkipResultsInDump:      2,
	}

	roundtripped, err := decodeCursor(encodeCursor(c))
	if err != nil {
		t.Fatalf("unexpected error decoding cursor: %s", err)
	}

	if !reflect.DeepEqual(c, roundtripped) {
		t.Errorf("unexpected cursor. want=%v have=%v", c, roundtripped)
	}
}

func TestDecodeFreshCursorFromRequest(t *testing.T) {
	dump := db.Dump{ID: 42, Root: "sub/"}
	moniker1 := bundles.MonikerData{Kind: "import", Scheme: "gomod", Identifier: "pad", PackageInformationID: "1234"}
	moniker2 := bundles.MonikerData{Kind: "export", Scheme: "gomod", Identifier: "pad", PackageInformationID: "1234"}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient := &mockBundleClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		if id == 42 {
			return dump, true, nil
		}
		return db.Dump{}, false, nil
	}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		if bundleID != 42 {
			t.Fatalf("unexpected bundle id. want=%v have=%v", 42, bundleID)
		}
		return mockBundleClient
	}

	mockBundleClient.monikersByPosition = func(ctx context.Context, path string, line, character int) ([][]bundles.MonikerData, error) {
		if path != "main.go" {
			t.Fatalf("unexpected path. want=%v have=%v", "main.go", path)
		}
		if line != 10 {
			t.Fatalf("unexpected line. want=%v have=%v", 10, line)
		}
		if character != 20 {
			t.Fatalf("unexpected character. want=%v have=%v", 20, character)
		}

		return [][]bundles.MonikerData{{moniker1}, {moniker2}}, nil
	}

	query := url.Values{
		"path":      []string{"sub/main.go"},
		"line":      []string{"10"},
		"character": []string{"20"},
		"uploadId":  []string{"42"},
	}

	expectedCursor := Cursor{
		Phase:     "same-dump",
		DumpID:    42,
		Path:      "main.go",
		Line:      10,
		Character: 20,
		Monikers:  []bundles.MonikerData{moniker1, moniker2},
	}

	if cursor, err := decodeCursorFromRequest(query, mockDB, mockBundleManagerClient); err != nil {
		t.Fatalf("unexpected error decoding cursor: %s", err)
	} else if !reflect.DeepEqual(cursor, expectedCursor) {
		t.Errorf("unexpected cursor. want=%v have=%v", expectedCursor, cursor)
	}
}

func TestDecodeFreshCursorFromRequestUnknownDump(t *testing.T) {
	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}

	mockDB.getDumpByID = func(ctx context.Context, id int) (db.Dump, bool, error) {
		return db.Dump{}, false, nil
	}

	query := url.Values{
		"path":      []string{"sub/main.go"},
		"line":      []string{"10"},
		"character": []string{"20"},
		"uploadId":  []string{"42"},
	}

	if _, err := decodeCursorFromRequest(query, mockDB, mockBundleManagerClient); err != ErrMissingDump {
		t.Fatalf("unexpected error decoding cursor. want=%v have =%v", ErrMissingDump, err)
	}
}

func TestDecodeExistingCursorFromRequest(t *testing.T) {
	expectedCursor := Cursor{
		Phase:     "same-repo",
		DumpID:    42,
		Path:      "/foo/bar/baz.go",
		Line:      10,
		Character: 50,
		Monikers: []bundles.MonikerData{
			{Kind: "k1", Scheme: "s1", Identifier: "i1", PackageInformationID: "pid1"},
			{Kind: "k2", Scheme: "s2", Identifier: "i2", PackageInformationID: "pid2"},
			{Kind: "k3", Scheme: "s3", Identifier: "i3", PackageInformationID: "pid3"},
		},
		SkipResults:            1,
		Identifier:             "x",
		Scheme:                 "gomod",
		Name:                   "leftpad",
		Version:                "0.1.0",
		DumpIDs:                []int{1, 2, 3, 4, 5},
		TotalDumpsWhenBatching: 5,
		SkipDumpsWhenBatching:  4,
		SkipDumpsInBatch:       3,
		SkipResultsInDump:      2,
	}

	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}

	query := url.Values{
		"cursor": []string{encodeCursor(expectedCursor)},
	}

	cursor, err := decodeCursorFromRequest(query, mockDB, mockBundleManagerClient)
	if err != nil {
		t.Fatalf("unexpected error decoding cursor: %s", err)
	} else if !reflect.DeepEqual(cursor, expectedCursor) {
		t.Errorf("unexpected cursor. want=%v have=%v", expectedCursor, cursor)
	}
}
