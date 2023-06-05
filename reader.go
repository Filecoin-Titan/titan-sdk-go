package titan

import (
	files "github.com/ipfs/go-ipfs-files"
	"io"
)

type fileReader struct {
	reader io.ReadCloser
	notify func()
}

func newFileReader(node files.Node, notifyFunc func()) *fileReader {
	file, _ := node.(files.File)
	return &fileReader{
		reader: file,
		notify: notifyFunc,
	}
}

func (f *fileReader) Read(p []byte) (int, error) {
	n, err := f.reader.Read(p)
	if err == io.EOF {
		f.notify()
	}

	return n, err
}

func (f *fileReader) Close() error {
	return f.reader.Close()
}

var _ io.ReadCloser = (*fileReader)(nil)
