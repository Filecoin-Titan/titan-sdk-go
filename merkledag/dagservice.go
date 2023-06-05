package merkledag

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/titan"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
)

var log = logging.Logger("dag-service")

type dagService struct {
	titan *titan.Service
}

// NewDAGService constructs a new NewDAGService (using the default implementation).
func NewDAGService(service *titan.Service) *dagService {
	return &dagService{
		titan: service,
	}
}

// Get retrieves a node from titan network
func (d *dagService) Get(ctx context.Context, cid cid.Cid) (ipld.Node, error) {
	block, err := d.titan.GetBlock(ctx, cid)
	if err != nil {
		return nil, errors.Errorf("dagService: get block %v", err)
	}

	return ipldlegacy.DecodeNode(ctx, block)
}

// GetMany gets many nodes at once, batching the request if possible.
func (d *dagService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	out := make(chan *ipld.NodeOption)

	for _, k := range cids {
		go func(k cid.Cid) {
			node, err := d.Get(ctx, k)
			out <- &ipld.NodeOption{
				Node: node,
				Err:  err,
			}
		}(k)
	}

	return out
}

// Add unimplemented
func (d *dagService) Add(ctx context.Context, node ipld.Node) error {
	return errors.New("unimplemented")
}

// AddMany unimplemented
func (d *dagService) AddMany(ctx context.Context, nodes []ipld.Node) error {
	return errors.New("unimplemented")
}

// Remove unimplemented
func (d *dagService) Remove(ctx context.Context, cid cid.Cid) error {
	return errors.New("unimplemented")
}

// RemoveMany unimplemented
func (d *dagService) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	return errors.New("unimplemented")
}

var _ ipld.DAGService = (*dagService)(nil)
