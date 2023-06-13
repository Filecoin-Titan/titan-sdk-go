package byterange

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"github.com/eikenb/pipeat"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
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
	directEdges, natTraversalEdges, err := r.titan.GroupByEdges(ctx, cid)
	if err != nil {
		return 0, nil, err
	}

	fileSize, err := r.getFileSize(ctx, cid, directEdges)
	if err != nil {
		return 0, nil, err
	}

	workerChan, err := r.makeWorkerChan(ctx, directEdges, natTraversalEdges)
	if err != nil {
		return 0, nil, err
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
		workers:   workerChan,
		resp:      make(chan response, len(workerChan)),
		backoff: &backoff{
			minDelay: minBackoffDelay,
			maxDelay: maxBackoffDelay,
		},
	}).run(ctx)

	return fileSize, reader, nil
}

func (r *Range) getFileSize(ctx context.Context, cid cid.Cid, edges []*types.Edge) (int64, error) {
	var (
		start    int64
		size     int64 = 1 << 10 // 1 KiB
		fileSize int64
	)

	for _, edge := range edges {
		client := &types.Client{
			Node:       edge,
			HttpClient: r.titan.GetDefaultClient(),
		}

		fs, _, err := r.titan.GetRange(ctx, client, cid, start, size)
		if err != nil {
			log.Errorf("get range failed: %v", err)
			continue
		}

		fileSize = fs
		break
	}

	return fileSize, nil
}

func (r *Range) makeWorkerChan(ctx context.Context, n1 []*types.Edge, n2 []*types.Edge) (chan worker, error) {
	size := len(n1) + len(n2)
	workerChan := make(chan worker, size)

	for _, edge := range n1 {
		workerChan <- worker{
			c: &types.Client{
				Node:       edge,
				HttpClient: r.titan.GetDefaultClient(),
			},
		}
	}

	go r.addNewWorker(ctx, workerChan, n2)

	return workerChan, nil
}

func (r *Range) addNewWorker(ctx context.Context, newWorkerChan chan worker, edges []*types.Edge) {
	_, err := r.titan.Discover()
	if err != nil {
		log.Warn(err)
		return
	}

	clients, err := r.titan.FilterAccessibleNodes(ctx, edges)
	if err != nil {
		log.Warn(err)
		return
	}

	for _, client := range clients {
		newWorkerChan <- worker{
			c: client,
		}
	}
}
