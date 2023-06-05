package byterange

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/eikenb/pipeat"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"io"
)

var log = logging.Logger("range")

type Range struct {
	titan       *titan.Service
	size        int64
	concurrency int
}

func New(service *titan.Service, size int64, concurrency int) *Range {
	return &Range{
		titan:       service,
		size:        size,
		concurrency: concurrency,
	}
}

func (r *Range) GetFile(ctx context.Context, cid cid.Cid) (int64, io.ReadCloser, error) {
	var (
		start int64
		size  int64 = 1 << 10 // 1 KiB
	)

	fileSize, _, err := r.titan.GetRange(ctx, cid, start, size)
	if err != nil {
		log.Errorf("get range failed: %v", err)
		return 0, nil, err
	}

	reader, writer, err := pipeat.Pipe()
	if err != nil {
		return 0, nil, err
	}

	(&dispatcher{
		cid:         cid,
		fileSize:    fileSize,
		rangeSize:   r.size,
		concurrency: r.concurrency,
		titan:       r.titan,
		reader:      reader,
		writer:      writer,
		workers:     make(chan worker, r.concurrency),
		resp:        make(chan response, r.concurrency),
	}).run(ctx)

	return fileSize, reader, nil
}
