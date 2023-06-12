package byterange

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/eikenb/pipeat"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
	"io"
	"time"
)

const (
	minBackoffDelay = 100 * time.Millisecond
	maxBackoffDelay = 3 * time.Second
)

var log = logging.Logger("range")

type Range struct {
	titan *titan.Service
	size  int64
}

func New(service *titan.Service, size int64) *Range {
	return &Range{
		titan: service,
		size:  size,
	}
}

func (r *Range) GetFile(ctx context.Context, cid cid.Cid) (int64, io.ReadCloser, error) {
	clients, err := r.titan.GetAccessibleEdges(ctx, cid)
	if err != nil {
		return 0, nil, err
	}

	if len(clients) == 0 {
		return 0, nil, errors.New("asset not found")
	}

	var (
		start    int64
		size     int64 = 1 << 10 // 1 KiB
		fileSize int64
	)

	for _, client := range clients {
		fileSize, _, err = r.titan.GetRange(ctx, client, cid, start, size)
		if err != nil {
			log.Errorf("get range failed: %v", err)
			continue
		}
		break
	}

	reader, writer, err := pipeat.Pipe()
	if err != nil {
		return 0, nil, err
	}

	(&dispatcher{
		cid:       cid,
		fileSize:  fileSize,
		rangeSize: r.size,
		titan:     r.titan,
		reader:    reader,
		writer:    writer,
		workers:   make(chan worker, len(clients)),
		resp:      make(chan response, len(clients)),
		clients:   clients,
		backoff: &backoff{
			minDelay: minBackoffDelay,
			maxDelay: maxBackoffDelay,
		},
	}).run(ctx)

	return fileSize, reader, nil
}
