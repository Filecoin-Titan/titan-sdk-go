package titan

import (
	"context"
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"net/http"
	"sync"
)

const (
	unknown        = types.NATUnknown
	openInternet   = types.NATOpenInternet
	symmetric      = types.NATSymmetric
	fullCone       = types.NATFullCone
	restricted     = types.NATRestricted
	portRestricted = types.NATPortRestricted
	udpBlock       = types.NATUDPBlock
)

const (
	minCandidatesOfDiscovery = 3
)

// Discover client-side NAT type discovery
func (s *Service) Discover() (t types.NATType, e error) {
	defer func() {
		s.mineNat = t
		log.Debugf("My NAT type: %s", t)
	}()

	schedulers, err := s.GetSchedulers()
	if err != nil {
		return unknown, errors.Errorf("get sechedulers: %v", err)
	}

	if len(schedulers) == 0 {
		return unknown, errors.Errorf("can not found scheudler")
	}

	candidates, err := s.GetCandidates(schedulers[0])
	if err != nil {
		return unknown, errors.Errorf("get candidates: %v", err)
	}

	if len(candidates) == 0 {
		return unknown, errors.Errorf("can not found candidates")
	}

	primaryCandidate := candidates[0]

	// Test I: sends an udp packet to primary candidates
	publicAddrPrimary, err := s.GetPublicAddress(primaryCandidate)
	if err != nil {
		return udpBlock, errors.Errorf("Test I: %v", err)
	}

	log.Debugf("PublicAddr: %s", publicAddrPrimary)

	if len(candidates) < minCandidatesOfDiscovery {
		return unknown, errors.Errorf("insufficent candidates, want %d got %d", minCandidatesOfDiscovery, len(candidates))
	}

	secondaryCandidate := candidates[1]
	tertiaryCandidate := candidates[2]

	// Test II: sends an udp packet to secondary candidates
	publicAddrSecondary, err := s.GetPublicAddress(secondaryCandidate)
	if err != nil {
		return unknown, errors.Errorf("Test II: %v", err)
	}

	if publicAddrPrimary.Port != publicAddrSecondary.Port {
		return symmetric, nil
	}

	var (
		isOpenInternet bool
		isFullCone     bool
		isRestricted   bool
	)

	todos := []func() error{
		func() error {
			// Test III: sends a tcp packet to primaryCandidate from tertiary candidates
			err = s.RequestCandidateToSendPackets(tertiaryCandidate, "tcp", publicAddrPrimary.String())
			if err != nil {
				return errors.Errorf("Test III failed: candidate: %s, %v", tertiaryCandidate, err)
			}

			isOpenInternet = true
			return nil
		},
		func() error {
			// Test IV: sends an udp packet to primaryCandidate from tertiary candidates
			err = s.RequestCandidateToSendPackets(tertiaryCandidate, "udp", publicAddrPrimary.String())
			if err != nil {
				return errors.Errorf("Test IV failed: candidate:%s %v", tertiaryCandidate, err)
			}

			isFullCone = true
			return nil
		},
		func() error {
			// Test V: sends an udp packet to primaryCandidate from primary candidates
			err = s.RequestCandidateToSendPackets(primaryCandidate, "udp", publicAddrPrimary.String())
			if err != nil {
				return errors.Errorf("Test V failed: %v", err)
			}

			isRestricted = true
			return nil
		},
	}

	var eg errgroup.Group
	for _, todo := range todos {
		eg.Go(todo)
	}
	if err = eg.Wait(); err != nil {
		log.Debugf("check list failed: %v", err)
	}

	if isOpenInternet {
		return openInternet, nil
	} else if isFullCone {
		return fullCone, nil
	} else if isRestricted {
		return restricted, nil
	} else {
		return portRestricted, nil
	}
}

// accessibleNodes filtering out the list of available edges to only include those that are accessible by the client
// and added to the list of accessible accessibleEdges.
func (s *Service) accessibleNodes(ctx context.Context, edges []*types.Edge) (map[string]*types.Client, error) {
	var (
		lk      sync.Mutex
		clients = make(map[string]*types.Client)
		wg      sync.WaitGroup
	)

	wg.Add(len(edges))
	for i := 0; i < len(edges); i++ {
		go func(edge *types.Edge) {
			defer wg.Done()

			if edge.GetNATType() == symmetric {
				log.Warnf("A symmetric type device was found, but we haven't implemented it yet, so skip that for now.")
				return
			}

			client, err := s.determineEdgeClient(ctx, s.mineNat, edge)
			if err != nil {
				log.Warnf("determine edge %s(%s) http client failed: %v", edge.NodeID, edge.Address, err)
				return
			}

			err = s.SendPackets(client, edge.Address)
			if err != nil {
				log.Warnf("send packets to edge %s(%s) failed: %v", edge.NodeID, edge.Address, err)
				return
			}

			lk.Lock()
			clients[edge.NodeID] = &types.Client{
				Node:       edge,
				HttpClient: client,
			}
			lk.Unlock()
		}(edges[i])
	}

	wg.Wait()

	return clients, nil
}

// determineEdgeClient determines that can be directly connected to using the default httpclient.
// If an edge is not directly accessible, attempts NAT traversal to see if the edge can be accessed that way.
// If NAT traversal is successful, the edge is wrapped into a new httpclient.
func (s *Service) determineEdgeClient(ctx context.Context, userNATType types.NATType, edge *types.Edge) (*http.Client, error) {
	edgeNATType := edge.GetNATType()

	// Check if the edge is already directly accessible
	if edgeNATType == openInternet || edgeNATType == fullCone {
		return s.httpClient, nil
	}

	// Check if the user has an open Internet NAT type, then try to establish a connection through NAT traversal
	if userNATType == openInternet || userNATType == fullCone {
		if err := s.EstablishConnectionFromEdge(edge); err != nil {
			return nil, errors.Errorf("establish connection from edge: %v", err)
		}

		return s.httpClient, nil
	}

	// Check if the edge and the user both have a restricted cone NAT type, then request the scheduler to connect to the edge node
	if edgeNATType == restricted || userNATType == restricted {
		err := s.EstablishConnectionFromEdge(edge)
		if err != nil {
			return nil, errors.Errorf("request candidate to send packets failed, edge: %s: err: %v", edge.Address, err)
		}

		client, err := newHttp3Client(ctx, s.conn, edge.Address, s.opts.Timeout)
		if err != nil {
			return nil, errors.Errorf("create new http3 client: %v", err)
		}

		return client, nil
	}

	// Check if the edge and the user both have a restricted port cone NAT type, then try to send packets to the edge and request the scheduler to do so as well
	if edgeNATType == portRestricted && userNATType == portRestricted {
		go s.SendPackets(s.httpClient, edge.Address)

		err := s.EstablishConnectionFromEdge(edge)
		if err != nil {
			return nil, errors.Errorf("request candidate to send packets failed, edge: %s, err: %v", edge.Address, err)
		}

		client, err := newHttp3Client(ctx, s.conn, edge.Address, s.opts.Timeout)
		if err != nil {
			return nil, errors.Errorf("create new http3 client: %v", err)
		}

		return client, nil
	}

	if edgeNATType == symmetric || userNATType == symmetric {
		// TODO: request the scheduler to send packets and guess the port
		return nil, errors.Errorf("symmetric NAT unimplemented")
	}

	return nil, errors.Errorf("unknown NAT type")
}
