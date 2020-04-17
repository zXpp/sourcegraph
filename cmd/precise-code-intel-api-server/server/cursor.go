package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

type Cursor struct {
	Phase                  string                // common
	DumpID                 int                   // common
	Path                   string                // same-dump/definition-monikers
	Line                   int                   // same-dump
	Character              int                   // same-dump
	Monikers               []bundles.MonikerData // same-dump/definition-monikers
	SkipResults            int                   // same-dump/definition-monikers
	Identifier             string                // same-repo/remote-repo
	Scheme                 string                // same-repo/remote-repo
	Name                   string                // same-repo/remote-repo
	Version                string                // same-repo/remote-repo
	DumpIDs                []int                 // same-repo/remote-repo
	TotalDumpsWhenBatching int                   // same-repo/remote-repo
	SkipDumpsWhenBatching  int                   // same-repo/remote-repo
	SkipDumpsInBatch       int                   // same-repo/remote-repo
	SkipResultsInDump      int                   // same-repo/remote-repo
}

func decodeCursor(rawEncoded string) (cursor Cursor, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(rawEncoded)
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(raw), &cursor)
	return
}

func encodeCursor(cursor Cursor) string {
	rawEncoded, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(rawEncoded)
}

// TODO - test
// TODO - move?
func decodeCursorFromRequest(r *http.Request, db db.DB, bundleManagerClient bundles.BundleManagerClient) (Cursor, error) {
	q := r.URL.Query()
	file := q.Get("path")
	line, _ := strconv.Atoi(q.Get("line"))
	character, _ := strconv.Atoi(q.Get("character"))
	uploadID, _ := strconv.Atoi(q.Get("uploadId"))
	encoded := q.Get("cursor")

	if encoded != "" {
		cursor, err := decodeCursor(encoded)
		if err != nil {
			return Cursor{}, err
		}

		return cursor, nil
	}

	dump, exists, err := db.GetDumpByID(context.Background(), uploadID)
	if err != nil {
		return Cursor{}, err
	}
	if !exists {
		return Cursor{}, ErrMissingDump
	}

	pathInBundle := strings.TrimPrefix(file, dump.Root)
	bundleClient := bundleManagerClient.BundleClient(dump.ID)

	rangeMonikers, err := bundleClient.MonikersByPosition(pathInBundle, line, character)
	if err != nil {
		return Cursor{}, err
	}

	var flattened []bundles.MonikerData
	for _, monikers := range rangeMonikers {
		flattened = append(flattened, monikers...)
	}

	return Cursor{
		Phase:       "same-dump",
		DumpID:      dump.ID,
		Path:        pathInBundle,
		Line:        line,
		Character:   character,
		Monikers:    flattened,
		SkipResults: 0,
	}, nil
}
