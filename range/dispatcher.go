package byterange

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/eikenb/pipeat"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"math"
)

type dispatcher struct {
	cid         cid.Cid
	fileSize    int64
	rangeSize   int64
	concurrency int
	todos       JobQueue
	workers     chan worker
	resp        chan response
	titan       *titan.Service
	writer      *pipeat.PipeWriterAt
	reader      *pipeat.PipeReaderAt
}

type worker struct {
	id int
}

type response struct {
	offset int64
	data   []byte
}

type job struct {
	index int
	start int64
	end   int64
	retry int
}

func (d *dispatcher) initialization() {
	for i := 0; i < d.concurrency; i++ {
		d.workers <- worker{id: i}
	}

	count := int64(math.Ceil(float64(d.fileSize) / float64(d.rangeSize)))
	for i := int64(0); i < count; i++ {
		start := i * d.rangeSize
		end := (i + 1) * d.rangeSize

		if end > d.fileSize {
			end = d.fileSize
		}

		d.todos.Push(&job{
			index: int(i),
			start: start,
			end:   end,
		})
	}
}

func (d *dispatcher) run(ctx context.Context) {
	d.initialization()
	d.writeData(ctx)

	var (
		counter  int64
		finished = make(chan int64, 1)
	)

	go func() {
		for {
			select {
			case w := <-d.workers:
				go func() {
					j, ok := d.todos.Pop()
					if !ok {
						d.workers <- w
						return
					}

					if j.retry > 0 {
						log.Debugf("pull data (retries: %d)", j.retry)
					}

					data, err := d.fetch(ctx, d.cid, j.start, j.end)
					if err != nil {
						log.Errorf("pull data failed: %v", err)

						if j.retry < 3 {
							j.retry++
						} else {
							// TODO: maybe remove bad nodes
						}

						d.todos.PushFront(j)
						d.workers <- w
						return
					}

					dataLen := j.end - j.start

					if int64(len(data)) < dataLen {
						log.Debugf("unexpected data size, want %d got %d", dataLen, len(data))
						d.todos.PushFront(j)
						d.workers <- w
						return
					}

					d.workers <- w
					d.resp <- response{
						data:   data[:dataLen],
						offset: j.start,
					}
					finished <- dataLen
				}()
			case size := <-finished:
				counter += size
				if counter >= d.fileSize {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return
}

func (d *dispatcher) writeData(ctx context.Context) {
	go func() {
		defer d.finally()

		var count int64
		for {
			select {
			case r := <-d.resp:
				_, err := d.writer.WriteAt(r.data, r.offset)
				if err != nil {
					log.Errorf("write data failed: %v", err)
					continue
				}

				count += int64(len(r.data))
				if count >= d.fileSize {
					return
				}
			case <-ctx.Done():
				return
			}
		}

	}()
}

func (d *dispatcher) fetch(ctx context.Context, cid cid.Cid, start, end int64) ([]byte, error) {
	_, data, err := d.titan.GetRange(ctx, cid, start, end)
	if err != nil {
		return nil, errors.Errorf("get range failed: %v", err)
	}
	return data, nil
}

func (d *dispatcher) finally() {
	if err := d.titan.EndOfFile(); err != nil {
		log.Errorf("end of file failed: %v", err)
	}

	if err := d.writer.Close(); err != nil {
		log.Errorf("close write failed: %v", err)
	}
}
