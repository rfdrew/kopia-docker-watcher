package kopiaclient

import (
	"context"
	"net/http"
)

// RepoStatus is the response of GET /api/v1/repo/status.
type RepoStatus struct {
	// Connected is false while kopia server has no open repository. Unlike
	// the other UI API calls, repo/status answers 200 then instead of 400
	// NOT_CONNECTED.
	Connected bool `json:"connected"`

	ConfigFile                 string `json:"configFile,omitempty"`
	FormatVersion              int    `json:"formatVersion,omitempty"`
	Hash                       string `json:"hash,omitempty"`
	Encryption                 string `json:"encryption,omitempty"`
	ECC                        string `json:"ecc,omitempty"`
	ECCOverheadPercent         int    `json:"eccOverheadPercent,omitempty"`
	Splitter                   string `json:"splitter,omitempty"`
	MaxPackSize                int    `json:"maxPackSize,omitempty"`
	Storage                    string `json:"storage,omitempty"`
	APIServerURL               string `json:"apiServerURL,omitempty"`
	SupportsContentCompression bool   `json:"supportsContentCompression"`

	// Client options of the connected repository: the identity kopia
	// server snapshots its local sources as.
	Hostname    string `json:"hostname,omitempty"`
	Username    string `json:"username,omitempty"`
	Description string `json:"description,omitempty"`
	ReadOnly    bool   `json:"readonly,omitempty"`

	// InitTaskID is set while the repository is being opened, created or
	// connected.
	InitTaskID string `json:"initTaskID,omitempty"`
}

// RepoStatus returns the repository status. It works without a connected
// repository.
func (c *Client) RepoStatus(ctx context.Context) (*RepoStatus, error) {
	var st RepoStatus
	if err := c.call(ctx, request{method: http.MethodGet, path: "repo/status"}, &st); err != nil {
		return nil, err
	}

	return &st, nil
}
