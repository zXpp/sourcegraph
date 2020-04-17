package server

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestReferences(t *testing.T) {
	// TODO
}

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
