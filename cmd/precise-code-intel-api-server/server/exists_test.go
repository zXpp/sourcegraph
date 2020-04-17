package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func TestFindClosestDatabase(t *testing.T) {
	mockDB := &mockDB{}
	mockBundleManagerClient := &mockBundleManagerClient{}
	mockBundleClient1 := &mockBundleClient{}
	mockBundleClient2 := &mockBundleClient{}
	mockBundleClient3 := &mockBundleClient{}
	mockBundleClient4 := &mockBundleClient{}

	mockDB.findClosestDumps = func(ctx context.Context, repositoryID int, commit, file string) ([]db.Dump, error) {
		if repositoryID != 42 {
			t.Errorf("unexpected repository id. want=%d have=%d", 42, repositoryID)
		}
		if commit != "deadbeef01deadbeef02deadbeef03deadbeef04" {
			t.Errorf("unexpected commit. want=%s have=%s", "deadbeef01deadbeef02deadbeef03deadbeef04", commit)
		}
		if file != "s1/main.go" {
			t.Errorf("unexpected file. want=%s have=%s", "s1/main.go", file)
		}
		return []db.Dump{
			{ID: 50, Root: "s1/"},
			{ID: 51, Root: "s1/"},
			{ID: 52, Root: "s1/"},
			{ID: 53, Root: "s2/"},
		}, nil
	}

	mockBundleManagerClient.bundleClient = func(bundleID int) bundles.BundleClient {
		switch bundleID {
		case 50:
			return mockBundleClient1
		case 51:
			return mockBundleClient2
		case 52:
			return mockBundleClient3
		case 53:
			return mockBundleClient4
		}

		t.Fatalf("unexpected bundle id %d", bundleID)
		return nil
	}

	mockBundleClient1.exists = func(path string) (bool, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%s have=%s", "main.go", path)
		}
		return true, nil
	}
	mockBundleClient2.exists = func(path string) (bool, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%s have=%s", "main.go", path)
		}
		return false, nil
	}
	mockBundleClient3.exists = func(path string) (bool, error) {
		if path != "main.go" {
			t.Errorf("unexpected path. want=%s have=%s", "main.go", path)
		}
		return true, nil
	}
	mockBundleClient4.exists = func(path string) (bool, error) {
		if path != "s1/main.go" {
			t.Errorf("unexpected path. want=%s have=%s", "main.go", path)
		}
		return false, nil
	}

	s := &Server{
		db:                  mockDB,
		bundleManagerClient: mockBundleManagerClient,
	}

	dumps, err := s.findClosestDatabase(42, "deadbeef01deadbeef02deadbeef03deadbeef04", "s1/main.go")
	if err != nil {
		t.Errorf("unexpected error finding closest database: %s", err)
	}

	expected := []db.Dump{
		{ID: 50, Root: "s1/"},
		{ID: 52, Root: "s1/"},
	}
	if !reflect.DeepEqual(dumps, expected) {
		t.Errorf("unexpected file. want=%v have=%v", expected, dumps)
	}
}
