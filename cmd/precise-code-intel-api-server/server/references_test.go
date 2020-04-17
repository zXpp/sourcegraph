package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestReferencesSameDumpPartial(t *testing.T) {
	dump := db.Dump{ID: 42}

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
			t.Fatalf("unexpected bundle id %d", bundleID)
		}

		return mockBundleClient
	}

	mockBundleClient.references = func(ctx context.Context, path string, line, character int) ([]bundles.Location, error) {
		// TODO
		return nil, nil
	}

	mockBundleClient.monikerResults = func(ctx context.Context, modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
		// TODO
		return nil, 0, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	references, newCursor, hasNewCursor, err := s.references(100, "deadbeef01deadbeef01deadbeef01deadbeef01", 10, Cursor{
		Phase:     "same-dump",
		DumpID:    42,
		Path:      "main.go",
		Line:      23,
		Character: 34,
		Monikers:  nil, // TODO
	})
	if err != nil {
		t.Fatalf("expected error getting references: %s", err)
	}

	expectedReferences := []ResolvedLocation{
		// {Dump: dump, Path: "sub/foo.go", Range: r1},
		// {Dump: dump, Path: "sub/bar.go", Range: r2},
		// {Dump: dump, Path: "sub/baz.go", Range: r3},
	}

	expectedNewCursor := Cursor{
		// TODO
	}

	if !reflect.DeepEqual(references, expectedReferences) {
		t.Errorf("unexpected references. want=%v have=%v", expectedReferences, references)
	}
	if !hasNewCursor {
		t.Errorf("expected new cursor")
	}
	if !reflect.DeepEqual(newCursor, expectedNewCursor) {
		t.Errorf("unexpected new cursor. want=%v have=%v", expectedNewCursor, newCursor)
	}
}

// TODO: TestReferencesSameDump
// TODO: TestReferencesDefinitionMonikersPartial
// TODO: TestReferencesDefinitionMonikers
// TODO: TestReferencesSameRepoPartial
// TODO: TestReferencesSameRepo
// TODO: TestReferencesRemoteRepoPartial
// TODO: TestReferencesRemoteRepo

func TestApplyBloomFilter(t *testing.T) {
	references := []db.Reference{
		{DumpID: 1, Filter: readTestFilter(t, "bar.1")},   // bar
		{DumpID: 2, Filter: readTestFilter(t, "bar.2")},   // no bar
		{DumpID: 3, Filter: readTestFilter(t, "bar.3")},   // bar
		{DumpID: 4, Filter: readTestFilter(t, "bar.4")},   // bar
		{DumpID: 5, Filter: readTestFilter(t, "bar.5")},   // no bar
		{DumpID: 6, Filter: readTestFilter(t, "bar.6")},   // bar
		{DumpID: 7, Filter: readTestFilter(t, "bar.7")},   // bar
		{DumpID: 8, Filter: readTestFilter(t, "bar.8")},   // no bar
		{DumpID: 9, Filter: readTestFilter(t, "bar.9")},   // bar
		{DumpID: 10, Filter: readTestFilter(t, "bar.10")}, // bar
		{DumpID: 11, Filter: readTestFilter(t, "bar.11")}, // no bar
		{DumpID: 12, Filter: readTestFilter(t, "bar.12")}, // bar
	}

	testCases := []struct {
		limit           int
		expectedScanned int
		expectedDumpIDs []int
	}{
		{1, 1, []int{1}},
		{2, 3, []int{1, 3}},
		{6, 9, []int{1, 3, 4, 6, 7, 9}},
		{7, 10, []int{1, 3, 4, 6, 7, 9, 10}},
		{8, 12, []int{1, 3, 4, 6, 7, 9, 10, 12}},
		{12, 12, []int{1, 3, 4, 6, 7, 9, 10, 12}},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("limit=%d", testCase.limit)

		t.Run(name, func(t *testing.T) {
			filteredReferences, scanned := applyBloomFilter(references, "bar", testCase.limit)
			if scanned != testCase.expectedScanned {
				t.Errorf("unexpected scanned. want=%d have=%d", testCase.expectedScanned, scanned)
			}

			var filteredDumpIDs []int
			for _, reference := range filteredReferences {
				filteredDumpIDs = append(filteredDumpIDs, reference.DumpID)
			}

			if !reflect.DeepEqual(filteredDumpIDs, testCase.expectedDumpIDs) {
				t.Errorf("unexpected filtered references ids. want=%v have=%v", testCase.expectedDumpIDs, filteredDumpIDs)
			}
		})
	}
}
