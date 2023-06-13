package byterange

import (
	"context"
	"fmt"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"github.com/eikenb/pipeat"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"math"
	"math/rand"
	"time"
)

type dispatcher struct {
	cid       cid.Cid
	fileSize  int64
	rangeSize int64
	todos     JobQueue
	workers   chan worker
	resp      chan response
	titan     *titan.Service
	writer    *pipeat.PipeWriterAt
	reader    *pipeat.PipeReaderAt
	backoff   *backoff
}

type worker struct {
	id int
	c  *types.Client
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

type backoff struct {
	minDelay time.Duration
	maxDelay time.Duration
}

func (b *backoff) next(attempt int) time.Duration {
	if attempt < 0 {
		return b.minDelay
	}

	minf := float64(b.minDelay)
	durf := minf * math.Pow(1.5, float64(attempt))
	durf = durf + rand.Float64()*minf

	delay := time.Duration(durf)
	if delay > b.maxDelay {
		return b.maxDelay
	}

	return delay
}

func (d *dispatcher) generateJobs() {
	count := int64(math.Ceil(float64(d.fileSize) / float64(d.rangeSize)))
	for i := int64(0); i < count; i++ {
		start := i * d.rangeSize
		end := (i + 1) * d.rangeSize

		if end > d.fileSize {
			end = d.fileSize
		}

		newJob := &job{
			index: int(i),
			start: start,
			end:   end,
		}

		d.todos.Push(newJob)
	}
}

func (d *dispatcher) run(ctx context.Context) {
	d.generateJobs()
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

					data, err := d.fetch(ctx, w.c, d.cid, j.start, j.end)
					if err != nil {
						errMsg := fmt.Sprintf("pull data failed : %v", err)
						if j.retry > 0 {
							log.Debugf("pull data failed (retries: %d): %v", j.retry, err)
							<-time.After(d.backoff.next(j.retry))
						}

						log.Warnf(errMsg)

						j.retry++
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

func (d *dispatcher) fetch(ctx context.Context, c *types.Client, cid cid.Cid, start, end int64) ([]byte, error) {
	_, data, err := d.titan.GetRange(ctx, c, cid, start, end)
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
