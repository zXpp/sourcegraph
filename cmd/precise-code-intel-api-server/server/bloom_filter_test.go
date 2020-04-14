package server

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

var testFiles = []string{
	"64kb.16",
	"64kb.08",
	"64kb.24",
	"64kb.32",
	"32kb.16",
	"32kb.08",
	"32kb.24",
	"32kb.32",
	"96kb.16",
	"96kb.08",
	"96kb.24",
	"96kb.32",
	"128kb.16",
	"128kb.08",
	"128kb.24",
	"128kb.32",
}

func TestHashKeyUUIDs(t *testing.T) {
	loremIpsum := readTestWords(t, "lorem-ipsum")
	corporateIpsum := readTestWords(t, "corporate-ipsum")

	for _, filename := range testFiles {
		content, err := ioutil.ReadFile(fmt.Sprintf("test-data/filters/%s", filename))
		if err != nil {
			t.Fatalf("failed to read test file %s", filename)
		}

		raw, err := hex.DecodeString(strings.TrimSpace(string(content)))
		if err != nil {
			t.Fatalf("failed to decode test file %s", filename)
		}

		buckets, m, k, err := decodeFilter(raw)
		if err != nil {
			t.Fatalf("failed to decode filter: %s", err)
		}

		for _, v := range loremIpsum {
			if !test(v, buckets, m, k) {
				t.Errorf("expected %s to be in bloom filter", v)
			}
		}

		for _, v := range corporateIpsum {
			if test(v, buckets, m, k) {
				t.Errorf("expected %s not to be in bloom filter", v)
			}
		}
	}
}

func readTestWords(t *testing.T, filename string) []string {
	content, err := ioutil.ReadFile(fmt.Sprintf("test-data/words/%s", filename))
	if err != nil {
		t.Fatal(err)
	}

	return strings.Split(strings.TrimSpace(string(content)), "\n")
}
