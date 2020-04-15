package bundles

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type BundleClient interface {
	Exists(path string) (bool, error)
	Definitions(path string, line, character int) ([]Location, error)
	References(path string, line, character int) ([]Location, error)
	Hover(path string, line, character int) (string, Range, bool, error)
	MonikersByPosition(path string, line, character int) ([][]MonikerData, error)
	MonikerResults(modelType, scheme, identifier string, skip, take int) ([]Location, int, error)
	PackageInformation(path, packageInformationID string) (PackageInformationData, error)
}

type bundleClientImpl struct {
	bundleManagerURL string
	bundleID         int
}

var _ BundleClient = &bundleClientImpl{}

func (c *bundleClientImpl) Exists(path string) (exists bool, err error) {
	err = c.request("exists", map[string]interface{}{"path": path}, &exists)
	return
}

func (c *bundleClientImpl) Definitions(path string, line, character int) (locations []Location, err error) {
	args := map[string]interface{}{
		"path":      path,
		"line":      line,
		"character": character,
	}

	err = c.request("definitions", args, &locations)
	c.addBundleIDToLocations(locations)
	return
}

func (c *bundleClientImpl) References(path string, line, character int) (locations []Location, err error) {
	args := map[string]interface{}{
		"path":      path,
		"line":      line,
		"character": character,
	}

	err = c.request("references", args, &locations)
	c.addBundleIDToLocations(locations)
	return
}

func (c *bundleClientImpl) Hover(path string, line, character int) (text string, r Range, exists bool, err error) {
	args := map[string]interface{}{
		"path":      path,
		"line":      line,
		"character": character,
	}

	var target json.RawMessage
	err = c.request("hover", args, &target)

	// TODO - gross
	if string(target) == "null" {
		exists = false
		return
	}
	exists = true

	payload := struct {
		Text  string `json:"text"`
		Range Range  `json:"range"`
	}{}
	err = json.Unmarshal(target, &payload)
	text = payload.Text
	r = payload.Range
	return
}

func (c *bundleClientImpl) MonikersByPosition(path string, line, character int) (target [][]MonikerData, err error) {
	args := map[string]interface{}{
		"path":      path,
		"line":      line,
		"character": character,
	}

	err = c.request("monikersByPosition", args, &target)
	return
}

func (c *bundleClientImpl) MonikerResults(modelType, scheme, identifier string, skip, take int) (locations []Location, count int, err error) {
	args := map[string]interface{}{
		"modelType":  modelType,
		"scheme":     scheme,
		"identifier": identifier,
	}
	if skip != 0 {
		args["skip"] = skip
	}
	if take != 0 {
		args["take"] = take
	}

	target := struct {
		Locations []Location `json:"locations"`
		Count     int        `json:"count"`
	}{}

	err = c.request("monikerResults", args, &target)
	locations = target.Locations
	count = target.Count
	c.addBundleIDToLocations(locations)
	return
}

func (c *bundleClientImpl) PackageInformation(path, packageInformationID string) (target PackageInformationData, err error) {
	args := map[string]interface{}{
		"path":                 path,
		"packageInformationId": packageInformationID,
	}

	err = c.request("packageInformation", args, &target)
	return
}

func (c *bundleClientImpl) request(path string, qs map[string]interface{}, target interface{}) error {
	values := url.Values{}
	for k, v := range qs {
		values[k] = []string{fmt.Sprintf("%v", v)}
	}

	url, err := url.Parse(fmt.Sprintf("%s/dbs/%d/%s", c.bundleManagerURL, c.bundleID, path))
	if err != nil {
		return err
	}
	url.RawQuery = values.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(&target)
}

func (c *bundleClientImpl) addBundleIDToLocations(locations []Location) {
	for i := range locations {
		locations[i].DumpID = c.bundleID
	}
}
