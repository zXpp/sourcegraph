package server

import (
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func lookupMoniker(db *db.DB, bundleManagerClient *bundles.BundleManagerClient, dumpID int, path string, moniker bundles.MonikerData, model string, skip, take *int) ([]ResolvedLocation, int, error) {
	if moniker.PackageInformationID == "" {
		return nil, 0, nil
	}

	pid, err := bundleManagerClient.BundleClient(dumpID).PackageInformation(path, moniker.PackageInformationID)
	if err != nil {
		return nil, 0, err
	}

	dump, exists, err := db.GetPackage(moniker.Scheme, pid.Name, pid.Version)
	if err != nil || !exists {
		return nil, 0, err
	}

	locations, count, err := bundleManagerClient.BundleClient(dump.ID).MonikerResults(model, moniker.Scheme, moniker.Identifier, skip, take)
	if err != nil {
		return nil, 0, err
	}

	return resolveLocationsWithDump(dump, locations), count, nil
}

func lookupPackageInformation(bundleManagerClient *bundles.BundleManagerClient, dumpID int, path string, moniker bundles.MonikerData) (bundles.PackageInformationData, bool, error) {
	if moniker.PackageInformationID == "" {
		return bundles.PackageInformationData{}, false, nil
	}

	pi, err := bundleManagerClient.BundleClient(dumpID).PackageInformation(path, moniker.PackageInformationID)
	if err != nil {
		return bundles.PackageInformationData{}, false, err
	}

	return pi, true, nil
}
