package server

import (
	"context"
	"strings"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
)

func (s *Server) hover(file string, line, character, uploadID int) (string, bundles.Range, bool, error) {
	dump, exists, err := s.db.GetDumpByID(context.Background(), uploadID)
	if err != nil {
		return "", bundles.Range{}, false, err
	}
	if !exists {
		return "", bundles.Range{}, false, ErrMissingDump
	}

	pathInBundle := strings.TrimPrefix(file, dump.Root)
	bundleClient := s.bundleManagerClient.BundleClient(dump.ID)

	text, rn, exists, err := bundleClient.Hover(pathInBundle, line, character)
	if err != nil {
		return "", bundles.Range{}, false, err
	}
	if exists {
		return text, rn, true, nil
	}

	definition, exists, err := s.definitionRaw(dump, bundleClient, pathInBundle, line, character)
	if err != nil || !exists {
		return "", bundles.Range{}, false, err
	}

	pathInDefinitionBundle := strings.TrimPrefix(definition.Path, definition.Dump.Root)
	definitionBundleClient := s.bundleManagerClient.BundleClient(definition.Dump.ID)

	return definitionBundleClient.Hover(pathInDefinitionBundle, definition.Range.Start.Line, definition.Range.Start.Character)
}
