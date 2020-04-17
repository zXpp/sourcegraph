package server

import (
	"context"
	"io"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
)

type mockBundleManagerClient struct {
	bundleClient func(bundleID int) bundles.BundleClient
	sendUpload   func(ctx context.Context, bundleID int, r io.Reader) error
}

var _ bundles.BundleManagerClient = &mockBundleManagerClient{}

func (bmc *mockBundleManagerClient) BundleClient(bundleID int) bundles.BundleClient {
	return bmc.bundleClient(bundleID)
}

func (bmc *mockBundleManagerClient) SendUpload(ctx context.Context, bundleID int, r io.Reader) error {
	return bmc.sendUpload(ctx, bundleID, r)
}

type mockBundleClient struct {
	exists             func(ctx context.Context, path string) (bool, error)
	definitions        func(ctx context.Context, path string, line, character int) ([]bundles.Location, error)
	references         func(ctx context.Context, path string, line, character int) ([]bundles.Location, error)
	hover              func(ctx context.Context, path string, line, character int) (string, bundles.Range, bool, error)
	monikersByPosition func(ctx context.Context, path string, line, character int) ([][]bundles.MonikerData, error)
	monikerResults     func(ctx context.Context, modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error)
	packageInformation func(ctx context.Context, path, packageInformationID string) (bundles.PackageInformationData, error)
}

var _ bundles.BundleClient = &mockBundleClient{}

func (bc *mockBundleClient) Exists(ctx context.Context, path string) (exists bool, err error) {
	return bc.exists(ctx, path)
}

func (bc *mockBundleClient) Definitions(ctx context.Context, path string, line, character int) ([]bundles.Location, error) {
	return bc.definitions(ctx, path, line, character)
}

func (bc *mockBundleClient) References(ctx context.Context, path string, line, character int) ([]bundles.Location, error) {
	return bc.references(ctx, path, line, character)
}

func (bc *mockBundleClient) Hover(ctx context.Context, path string, line, character int) (string, bundles.Range, bool, error) {
	return bc.hover(ctx, path, line, character)
}

func (bc *mockBundleClient) MonikersByPosition(ctx context.Context, path string, line, character int) ([][]bundles.MonikerData, error) {
	return bc.monikersByPosition(ctx, path, line, character)
}

func (bc *mockBundleClient) MonikerResults(ctx context.Context, modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
	return bc.monikerResults(ctx, modelType, scheme, identifier, skip, take)
}

func (bc *mockBundleClient) PackageInformation(ctx context.Context, path, packageInformationID string) (bundles.PackageInformationData, error) {
	return bc.packageInformation(ctx, path, packageInformationID)
}
