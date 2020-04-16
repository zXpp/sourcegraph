package server

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestTestTypeScriptGeneratedBloomFilters(t *testing.T) {
	testCases := []struct {
		filterFile  string
		includeFile string
		excludeFile string
	}{
		{filterFile: "64kb.16", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "64kb.08", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "64kb.24", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "64kb.32", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "32kb.16", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "32kb.08", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "32kb.24", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "32kb.32", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "96kb.16", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "96kb.08", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "96kb.24", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "96kb.32", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "128kb.16", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "128kb.08", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "128kb.24", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "128kb.32", includeFile: "lorem-ipsum", excludeFile: "corporate-ipsum"},
		{filterFile: "emojis", includeFile: "emojis", excludeFile: "lorem-ipsum"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.filterFile, func(t *testing.T) {
			content, err := ioutil.ReadFile(fmt.Sprintf("test-data/filters/%s", testCase.filterFile))
			if err != nil {
				t.Fatalf("unexpected error reading: %s", err)
			}

			raw, err := hex.DecodeString(strings.TrimSpace(string(content)))
			if err != nil {
				t.Fatalf("unexpected error decoding: %s", err)
			}

			buckets, m, k, err := decodeFilter(raw)
			if err != nil {
				t.Fatalf("unexpected error decoding filter: %s", err)
			}

			for _, v := range readTestWords(t, testCase.includeFile) {
				if !test(v, buckets, m, k) {
					t.Errorf("expected %s to be in bloom filter", v)
				}
			}

			for _, v := range readTestWords(t, testCase.excludeFile) {
				if test(v, buckets, m, k) {
					t.Errorf("expected %s not to be in bloom filter", v)
				}
			}
		})
	}
}

func readTestWords(t *testing.T, filename string) []string {
	content, err := ioutil.ReadFile(fmt.Sprintf("test-data/words/%s", filename))
	if err != nil {
		t.Fatalf("unexpected error reading %s: %s", filename, err)
	}

	return strings.Split(strings.TrimSpace(string(content)), "\n")
}
