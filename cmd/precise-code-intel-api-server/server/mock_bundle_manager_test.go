package server

import (
	"io"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
)

type mockBundleManagerClient struct {
	bundleClient func(bundleID int) bundles.BundleClient
	sendUpload   func(bundleID int, r io.Reader) error
}

var _ bundles.BundleManagerClient = &mockBundleManagerClient{}

func (bmc *mockBundleManagerClient) BundleClient(bundleID int) bundles.BundleClient {
	return bmc.bundleClient(bundleID)
}

func (bmc *mockBundleManagerClient) SendUpload(bundleID int, r io.Reader) error {
	return bmc.sendUpload(bundleID, r)
}

type mockBundleClient struct {
	exists             func(path string) (bool, error)
	definitions        func(path string, line, character int) ([]bundles.Location, error)
	references         func(path string, line, character int) ([]bundles.Location, error)
	hover              func(path string, line, character int) (string, bundles.Range, bool, error)
	monikersByPosition func(path string, line, character int) ([][]bundles.MonikerData, error)
	monikerResults     func(modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error)
	packageInformation func(path, packageInformationID string) (bundles.PackageInformationData, error)
}

var _ bundles.BundleClient = &mockBundleClient{}

func (bc *mockBundleClient) Exists(path string) (exists bool, err error) {
	return bc.exists(path)
}

func (bc *mockBundleClient) Definitions(path string, line, character int) ([]bundles.Location, error) {
	return bc.definitions(path, line, character)
}

func (bc *mockBundleClient) References(path string, line, character int) ([]bundles.Location, error) {
	return bc.references(path, line, character)
}

func (bc *mockBundleClient) Hover(path string, line, character int) (string, bundles.Range, bool, error) {
	return bc.hover(path, line, character)
}

func (bc *mockBundleClient) MonikersByPosition(path string, line, character int) ([][]bundles.MonikerData, error) {
	return bc.monikersByPosition(path, line, character)
}

func (bc *mockBundleClient) MonikerResults(modelType, scheme, identifier string, skip, take int) ([]bundles.Location, int, error) {
	return bc.monikerResults(modelType, scheme, identifier, skip, take)
}

func (bc *mockBundleClient) PackageInformation(path, packageInformationID string) (bundles.PackageInformationData, error) {
	return bc.packageInformation(path, packageInformationID)
}
