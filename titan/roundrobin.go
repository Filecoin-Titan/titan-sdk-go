package titan

import (
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"math/rand"
	"sync"
)

type Strategy func(edges []*types.Client) func() *types.Client

// roundRobinSelector is a round-robin strategy algorithm for node selection.
func roundRobinSelector(edges []*types.Client) func() *types.Client {
	var nodes []*types.Client

	for _, node := range edges {
		nodes = append(nodes, node)
	}

	var i = rand.Intn(100)
	var mtx sync.Mutex

	return func() *types.Client {
		i++

		mtx.Lock()
		node := nodes[i%len(nodes)]
		mtx.Unlock()

		return node
	}
}
