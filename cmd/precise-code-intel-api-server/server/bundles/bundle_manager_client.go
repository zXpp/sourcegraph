package bundles

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type BundleManagerClient interface {
	BundleClient(bundleID int) BundleClient
	SendUpload(bundleID int, r io.Reader) error
}

type bundleManagerClientImpl struct {
	bundleManagerURL string
}

var _ BundleManagerClient = &bundleManagerClientImpl{}

func New(bundleManagerURL string) BundleManagerClient {
	return &bundleManagerClientImpl{bundleManagerURL: bundleManagerURL}
}

func (c *bundleManagerClientImpl) BundleClient(bundleID int) BundleClient {
	return &bundleClientImpl{
		bundleManagerURL: c.bundleManagerURL,
		bundleID:         bundleID,
	}
}

func (c *bundleManagerClientImpl) SendUpload(bundleID int, r io.Reader) error {
	url, err := url.Parse(fmt.Sprintf("%s/uploads/%d", c.bundleManagerURL, bundleID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), r)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status %d", resp.StatusCode)
	}

	return nil
}
