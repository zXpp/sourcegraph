package server

import (
	"context"
	"strings"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func (s *Server) findClosestDumps(repositoryID int, commit, file string) ([]db.Dump, error) {
	candidates, err := s.db.FindClosestDumps(context.Background(), repositoryID, commit, file)
	if err != nil {
		return nil, err
	}

	var dumps []db.Dump
	for _, dump := range candidates {
		exists, err := s.bundleManagerClient.BundleClient(dump.ID).Exists(context.Background(), strings.TrimPrefix(file, dump.Root))
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}

		dumps = append(dumps, dump)
	}

	return dumps, nil
}
