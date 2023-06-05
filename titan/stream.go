package titan

import (
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	"io"
	"net/http"
	"net/url"
	"path"
)

type StreamType string

const (
	Null       StreamType = "null"
	PushStream StreamType = "push"
)

// ReaderStream indicating the uuid of push request, ex: `ReaderStream{ Type: "push", Info: "[UUID string]" }
//
//	See https://github.com/Filecoin-Titan/titan/blob/main/lib/rpcenc/reader.go#L50
type ReaderStream struct {
	Type StreamType
	Info string
}

func pushStream(client *http.Client, pushURL string, r io.Reader) (ReaderStream, error) {
	reqID := uuid.New()
	u, err := url.Parse(pushURL)
	if err != nil {
		return ReaderStream{}, xerrors.Errorf("parsing push address: %w", err)
	}
	u.Path = path.Join(u.Path, reqID.String())

	go func() {
		// TODO: figure out errors here
		for {
			req, err := http.NewRequest("HEAD", u.String(), nil)
			if err != nil {
				log.Errorf("sending HEAD request for the reder param: %+v", err)
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := client.Do(req)
			if err != nil {
				log.Errorf("sending reader param: %+v", err)
				return
			}
			// todo do we need to close the body for a head request?

			if resp.StatusCode == http.StatusFound {
				nextStr := resp.Header.Get("Location")
				u, err = url.Parse(nextStr)
				if err != nil {
					log.Errorf("sending HEAD request for the reder param, parsing next url (%s): %+v", nextStr, err)
					return
				}

				continue
			}

			if resp.StatusCode == http.StatusNoContent { // reader closed before reading anything
				// todo just return??
				return
			}

			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				log.Errorf("sending reader param (%s): non-200 status: %s, msg: '%s'", u.String(), resp.Status, string(b))
				return
			}

			break
		}

		// now actually send the data
		req, err := http.NewRequest("POST", u.String(), r)
		if err != nil {
			log.Errorf("sending reader param: %+v", err)
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("sending reader param: %+v", err)
			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			log.Errorf("sending reader param (%s): non-200 status: %s, msg: '%s'", u.String(), resp.Status, string(b))
			return
		}
	}()

	return ReaderStream{Type: PushStream, Info: reqID.String()}, nil
}
