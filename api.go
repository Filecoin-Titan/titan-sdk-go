package titan

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/config"
	"github.com/Filecoin-Titan/titan-sdk-go/merkledag"
	"github.com/Filecoin-Titan/titan-sdk-go/notify"
	byteRange "github.com/Filecoin-Titan/titan-sdk-go/range"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/pkg/errors"
	"io"
)

type API interface {
	// GetFile get a file from the titan network.
	// The file is downloaded in chunks and assembled locally.
	GetFile(ctx context.Context, cid string) (int64, io.ReadCloser, error)
}

type Client struct {
	config config.Config
	titan  *titan.Service
	dag    ipld.DAGService
	notify *notify.Notification
}

func New(opts ...config.Option) (*Client, error) {
	options := config.DefaultOption()

	for _, opt := range opts {
		opt(&options)
	}

	s, err := titan.New(options)
	if err != nil {
		return nil, err
	}

	c := &Client{
		config: options,
		titan:  s,
		notify: notify.NewNotification(),
	}

	if options.Mode == config.TraversalModeDFS {
		c.dag = merkledag.NewDAGService(c.titan)
	}

	if options.Verbose {
		lvl, err := logging.LevelFromString("debug")
		if err != nil {
			return nil, err
		}
		logging.SetAllLoggers(lvl)
	}

	go c.notify.ListenEndOfFile(context.Background(), c.titan.EndOfFile)

	return c, nil
}

func (c *Client) Close() error {
	return c.titan.Close()
}

func (c *Client) GetTitanService() *titan.Service {
	return c.titan
}

func (c *Client) GetFile(ctx context.Context, id string) (int64, io.ReadCloser, error) {
	switch c.config.Mode {
	case config.TraversalModeDFS:
		return c.getFileByDFS(ctx, id)
	case config.TraversalModeRange:
		return c.getFileByRange(ctx, id)
	default:
		return 0, nil, errors.Errorf("unsupported traversal mode")
	}
}

func (c *Client) getFileByDFS(ctx context.Context, id string) (int64, io.ReadCloser, error) {
	cid, err := cid.Decode(id)
	if err != nil {
		return 0, nil, err
	}

	merkleNode, err := c.dag.Get(ctx, cid)
	if err != nil {
		return 0, nil, err
	}

	node, err := unixfile.NewUnixfsFile(ctx, c.dag, merkleNode)
	if err != nil {
		return 0, nil, err
	}

	size, err := node.Size()
	if err != nil {
		return 0, nil, err
	}

	switch node.(type) {
	case files.File:
		return size, newFileReader(node, c.notify.SendEndOfFileEvent), nil
	case files.Directory:
		return 0, nil, errors.Errorf("the merkle dag is directory")
	default:
		return 0, nil, errors.Errorf("operation not supported")
	}
}

func (c *Client) getFileByRange(ctx context.Context, id string) (int64, io.ReadCloser, error) {
	cid, err := cid.Decode(id)
	if err != nil {
		return 0, nil, err
	}

	r := byteRange.New(c.titan,
		c.config.RangeSize,
	)

	return r.GetFile(ctx, cid)
}

var _ API = (*Client)(nil)
