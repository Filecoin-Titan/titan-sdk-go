package titan

import (
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"math/rand"
	"sync"
)

type Strategy func(edges []*types.Edge) func() *types.Edge

// roundRobinSelector is a round-robin strategy algorithm for node selection.
func roundRobinSelector(edges []*types.Edge) func() *types.Edge {
	var nodes []*types.Edge

	for _, node := range edges {
		nodes = append(nodes, node)
	}

	var i = rand.Intn(100)
	var mtx sync.Mutex

	return func() *types.Edge {
		if len(nodes) == 0 {
			return nil
		}
		mtx.Lock()
		node := nodes[i%len(nodes)]
		mtx.Unlock()

		return node
	}
}
