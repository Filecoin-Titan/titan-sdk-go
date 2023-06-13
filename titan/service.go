package titan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Filecoin-Titan/titan-sdk-go/config"
	"github.com/Filecoin-Titan/titan-sdk-go/internal/codec"
	"github.com/Filecoin-Titan/titan-sdk-go/internal/crypto"
	"github.com/Filecoin-Titan/titan-sdk-go/internal/request"
	"github.com/Filecoin-Titan/titan-sdk-go/types"
	"github.com/docker/go-units"
	"github.com/gorilla/mux"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/sync/errgroup"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	formatRaw = "raw"
	formatCAR = "car"
	namespace = "ipfs"
)

const (
	natTTLInterval = 2 * time.Hour
)

var log = logging.Logger("service")

type Service struct {
	opts       config.Config
	httpClient *http.Client
	conn       net.PacketConn
	userNat    types.NATType
	lastNatT   time.Time
	ids        sync.Map
	counter    atomic.Int32

	prepareChan chan *types.ProofParam
	submitChan  chan struct{}

	nodes []*types.Client
}

type params []interface{}

func New(options config.Config) (*Service, error) {
	if options.Address == "" {
		return nil, errors.Errorf("address is empty")
	}

	conn, err := net.ListenPacket("udp4", options.ListenAddr)
	if err != nil {
		return nil, err
	}

	s := &Service{
		opts:        options,
		httpClient:  defaultHttpClient(conn, options.Timeout),
		conn:        conn,
		nodes:       make([]*types.Client, 0),
		prepareChan: make(chan *types.ProofParam, 1),
		submitChan:  make(chan struct{}, 0),
	}

	go serverHTTP(conn)
	go serverTCP(conn)
	go s.handleProofs()

	return s, nil
}

func (s *Service) Close() error {
	return s.conn.Close()
}

func getRpcV0URL(baseURL string) string {
	return fmt.Sprintf("%s/rpc/v0", baseURL)
}

func serverHTTP(conn net.PacketConn) {
	handler := mux.NewRouter()
	handler.HandleFunc("/ping", func(writer http.ResponseWriter, h *http.Request) {
		writer.Write([]byte("pong"))
	})

	tlsConf, err := generateTLSConfig()
	if err != nil {
		log.Errorf("http3 server create TLS configure failed: %v", err)
	}

	(&http3.Server{
		TLSConfig: tlsConf,
		Handler:   handler,
	}).Serve(conn)

}

func serverTCP(conn net.PacketConn) {
	srv := &http.Server{
		ReadHeaderTimeout: 30 * time.Second,
	}

	log.Debugf("listen tcp on: %s", conn.LocalAddr().String())
	ln, le := net.Listen("tcp", conn.LocalAddr().String())
	if le != nil {
		log.Errorf("tcp listen failed: %v", le)
	}
	srv.Serve(ln)
}

func (s *Service) GetDefaultClient() *http.Client {
	return s.httpClient
}

// GetBlock retrieves a raw block from titan http gateway
func (s *Service) GetBlock(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	_, err := s.GetAccessibleEdges(ctx, cid)
	if err != nil {
		return nil, err
	}

	if len(s.nodes) == 0 {
		return nil, errors.Errorf("no edge node found for cid: %s", cid.String())
	}

	edge, err := s.selectEdge()
	if err != nil {
		log.Warnf("selectEdge: %v", err)
		return nil, err
	}

	start := time.Now()
	size, data, err := PullData(ctx, edge.HttpClient, edge.Node, cid.String(), formatRaw, nil)
	if err != nil {
		return nil, errors.Errorf("post request failed: %v", err)
	}

	proofs := &proofOfWorkParams{
		cid:    cid,
		tStart: start.UnixMilli(),
		tEnd:   time.Now().UnixMilli(),
		size:   size,
		edge:   edge.Node,
	}

	if err = s.generateProofOfWork(proofs); err != nil {
		return nil, errors.Errorf("generate proof of work failed: %v", err)
	}

	return blocks.NewBlock(data), nil
}

func (s *Service) selectEdge() (*types.Client, error) {
	if len(s.nodes) == 0 {
		return nil, errors.Errorf("no accessible node")
	}

	luckyEdge := roundRobinSelector(s.nodes)()
	if luckyEdge == nil {
		return nil, errors.Errorf("unavaliable node")
	}

	return luckyEdge, nil
}

func PullData(ctx context.Context, client *http.Client, edge *types.Edge, cid string, format string, requestHeader http.Header) (int64, []byte, error) {
	startTime := time.Now()

	body, err := codec.Encode(edge.Token)
	if err != nil {
		return 0, nil, errors.Errorf("send request: %v", err)
	}

	ns := namespace + "/" + cid
	resp, err := request.NewBuilder(client, edge.Address, ns, requestHeader).
		Option("format", format).
		BodyBytes(body).Get(ctx)
	if err != nil {
		return 0, nil, errors.Errorf("send request: %v", err)
	}
	defer resp.Close()

	data, err := io.ReadAll(resp.Output)
	if err != nil {
		return 0, nil, err
	}

	size := int64(len(data))

	if resp.Header.Get("Content-Range") != "" {
		size, err = getFileSizeFromContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return 0, nil, err
		}
	}

	log.Debugf("pulling data from %s(%s) speed: %s/s", edge.NodeID, edge.Address, units.BytesSize(float64(len(data))/time.Since(startTime).Seconds()))

	return size, data, nil
}

func getFileSizeFromContentRange(contentRange string) (int64, error) {
	subs := strings.Split(contentRange, "/")
	if len(subs) != 2 {
		return 0, fmt.Errorf("invalid content range: %s", contentRange)
	}

	return strconv.ParseInt(subs[1], 10, 64)
}

func (s *Service) GetAccessibleEdges(ctx context.Context, cid cid.Cid) (map[string]*types.Client, error) {
	_, err := s.Discover()
	if err != nil {
		return nil, err
	}

	all, err := s.GetEdgeNodesByFile(cid)
	if err != nil {
		return nil, err
	}

	clients, err := s.FilterAccessibleNodes(ctx, all)
	if err != nil {
		return nil, err
	}

	for _, client := range clients {
		s.nodes = append(s.nodes, client)
	}

	return clients, nil
}

func (s *Service) GroupByEdges(ctx context.Context, cid cid.Cid) ([]*types.Edge, []*types.Edge, error) {
	var directlyConnEdges, natTraversalEdges []*types.Edge

	all, err := s.GetEdgeNodesByFile(cid)
	if err != nil {
		return nil, nil, err
	}

	if len(all) == 0 {
		return nil, nil, errors.New("asset not found")
	}

	for _, edge := range all {
		if edge.GetNATType() == openInternet || edge.GetNATType() == fullCone {
			directlyConnEdges = append(directlyConnEdges, edge)
			continue
		}
		natTraversalEdges = append(natTraversalEdges, edge)
	}

	return directlyConnEdges, natTraversalEdges, nil
}

// GetRange retrieves specific byte ranges of UnixFS files and raw blocks.
func (s *Service) GetRange(ctx context.Context, c *types.Client, cid cid.Cid, start, end int64) (int64, []byte, error) {
	startTime := time.Now()
	header := http.Header{}
	header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	size, data, err := PullData(ctx, c.HttpClient, c.Node, cid.String(), formatCAR, header)
	if err != nil {
		return 0, nil, errors.Errorf("post request failed, ip: %s, err: %v", c.Node.Address, err)
	}

	proofs := &proofOfWorkParams{
		cid:    cid,
		tStart: startTime.UnixMilli(),
		tEnd:   time.Now().UnixMilli(),
		size:   int64(len(data)),
		edge:   c.Node,
		rStart: start,
		rEnd:   end,
	}

	if err = s.generateProofOfWork(proofs); err != nil {
		return 0, nil, errors.Errorf("generate proof of work failed: %v", err)
	}

	return size, data, nil
}

type proofOfWorkParams struct {
	cid    cid.Cid
	tStart int64
	tEnd   int64
	size   int64
	edge   *types.Edge
	rStart int64
	rEnd   int64
}

// generateProofOfWork generates proofs of work for per request.
func (s *Service) generateProofOfWork(params *proofOfWorkParams) error {
	s.prepareChan <- &types.ProofParam{
		Proofs: &types.WorkloadReport{
			TokenID: params.edge.Token.ID,
			NodeID:  params.edge.NodeID,
			Workload: &types.Workload{
				StartTime:    params.tStart,
				EndTime:      params.tEnd,
				DownloadSize: params.size,
			},
			Extra: &types.Extra{
				Cost:    params.tEnd - params.tStart,
				Count:   1,
				Address: params.edge.Address,
			},
		},
		SchedulerURL: params.edge.SchedulerURL,
		SchedulerKey: params.edge.SchedulerKey,
	}
	return nil
}

func (s *Service) GetEdgeNodesByFile(cid cid.Cid) ([]*types.Edge, error) {
	serializedParams, err := json.Marshal(params{cid.String()})
	if err != nil {
		return nil, errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.EdgeDownloadInfos",
		Params:  serializedParams,
	}

	header := http.Header{}
	if s.opts.Token != "" {
		header.Add("Authorization", "Bearer "+s.opts.Token)
	}
	data, err := request.PostJsonRPC(s.httpClient, getRpcV0URL(s.opts.Address), req, header)
	if err != nil {
		return nil, errors.Errorf("post jsonrpc failed: %v", err)
	}

	var list []*types.EdgeDownloadInfoList
	if err = json.Unmarshal(data, &list); err != nil {
		return nil, err
	}

	var out []*types.Edge
	for _, item := range list {
		for _, edge := range item.Infos {
			e := &types.Edge{
				NodeID:       edge.NodeID,
				Address:      edge.Address,
				Token:        edge.Tk,
				NATType:      edge.NatType,
				SchedulerURL: item.SchedulerURL,
				SchedulerKey: item.SchedulerKey,
			}
			log.Debugf("edge node id: %s(%s) NAT: %s", e.NodeID, e.Address, e.NATType)
			out = append(out, e)
		}
	}

	return out, err
}

// GetSchedulers get scheduler list in the same region
func (s *Service) GetSchedulers() ([]string, error) {
	serializedParams, err := json.Marshal(params{""})
	if err != nil {
		return nil, errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.GetUserAccessPoint",
		Params:  serializedParams,
	}

	header := http.Header{}
	if s.opts.Token != "" {
		header.Add("Authorization", "Bearer "+s.opts.Token)
	}
	data, err := request.PostJsonRPC(s.httpClient, getRpcV0URL(s.opts.Address), req, header)
	if err != nil {
		return nil, err
	}

	var out types.AccessPoint
	err = json.Unmarshal(data, &out)

	return out.SchedulerURLs, nil
}

// GetCandidates get candidates list in the same region
func (s *Service) GetCandidates(schedulerURL string) ([]string, error) {
	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.GetCandidateURLsForDetectNat",
		Params:  nil,
	}

	data, err := request.PostJsonRPC(s.httpClient, schedulerURL, req, nil)
	if err != nil {
		return nil, err
	}

	var out []string
	err = json.Unmarshal(data, &out)

	return out, nil
}

// GetPublicAddress return the public address
func (s *Service) GetPublicAddress(schedulerURL string) (types.Host, error) {
	serializedParams, err := json.Marshal(params{})
	if err != nil {
		return types.Host{}, errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.GetExternalAddress",
		Params:  serializedParams,
	}

	data, err := request.PostJsonRPC(s.httpClient, schedulerURL, req, nil)
	if err != nil {
		return types.Host{}, err
	}

	subs := strings.Split(strings.Trim(string(data), "\""), ":")
	if len(subs) != 2 {
		return types.Host{}, errors.Errorf("invalid address: %s", subs)
	}

	return types.Host{
		IP:   subs[0],
		Port: subs[1],
	}, nil
}

// RequestCandidateToSendPackets sends packet from server side to determine the application connectivity
func (s *Service) RequestCandidateToSendPackets(remoteAddr string, network, url string) error {
	reqURL := fmt.Sprintf("https://%s/ping", url)
	serializedParams, err := json.Marshal(params{
		network, reqURL,
	})
	if err != nil {
		return errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.CheckNetworkConnectivity",
		Params:  serializedParams,
	}

	_, err = request.PostJsonRPC(s.httpClient, remoteAddr, req, nil)
	if err != nil {
		return errors.Errorf("request candidate to send packets failed: %v", err)
	}

	return err
}

// NatPunching creates a connection from edge node side for the application via the scheduler
func (s *Service) NatPunching(edge *types.Edge) error {
	serializedParams, err := json.Marshal(params{edge.ToNatPunchReq()})
	if err != nil {
		return errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.NatPunch",
		Params:  serializedParams,
	}

	_, err = request.PostJsonRPC(s.httpClient, edge.SchedulerURL, req, nil)
	if err != nil {
		return errors.Errorf("invoke titan.NatPunch: %v", err)
	}

	return err
}

// Version get titan server version
func (s *Service) Version(client *http.Client, remoteAddr string) error {
	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.Version",
		Params:  nil,
	}

	rpcURL := getRpcV0URL(remoteAddr)
	_, err := request.PostJsonRPC(client, rpcURL, req, nil)
	if err != nil {
		return errors.Errorf("send packet failed: %v", err)
	}

	return err
}

// SubmitProofOfWork submits a proof of work for a downloaded file
func (s *Service) SubmitProofOfWork(schedulerAddr string, data []byte) error {
	pushURL, err := getPushURL(schedulerAddr)
	if err != nil {
		return err
	}

	streamReader, err := pushStream(s.httpClient, pushURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	serializedParams, err := json.Marshal(params{streamReader})
	if err != nil {
		return errors.Errorf("marshaling params failed: %v", err)
	}

	req := request.Request{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  "titan.SubmitUserWorkloadReport",
		Params:  serializedParams,
	}

	_, err = request.PostJsonRPC(s.httpClient, schedulerAddr, req, nil)
	if err != nil {
		return errors.Errorf("submitting proof of work failed: %v", err)
	}

	return nil
}

func getPushURL(addr string) (string, error) {
	pushURL, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushURL.Scheme {
	case "ws":
		pushURL.Scheme = "http"
	case "wss":
		pushURL.Scheme = "https"
	}
	///rpc/v0 -> /rpc/streams/v0/push

	pushURL.Path = path.Join(pushURL.Path, "../streams/v0/push")
	return pushURL.String(), nil
}

func (s *Service) EndOfFile() error {
	s.submitChan <- struct{}{}
	return nil
}

func encrypt(key string, value interface{}) ([]byte, error) {
	data, err := codec.Encode(value)
	if err != nil {
		return nil, err
	}

	pub, err := crypto.DecodePublicKey(key)
	if err != nil {
		return nil, err
	}

	return crypto.Encrypt(data, pub)
}

func printReport(proofs map[string]*types.ProofParam) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"NodeID", "Address", "Speed", "Count", "DataSize"})

	for _, item := range proofs {
		speed := fmt.Sprintf("%s/s", units.BytesSize(float64(item.Proofs.Workload.DownloadSpeed)))
		size := units.BytesSize(float64(item.Proofs.Workload.DownloadSize))
		count := strconv.Itoa(int(item.Proofs.Extra.Count))
		table.Append([]string{item.Proofs.NodeID, item.Proofs.Extra.Address, speed, count, size})
	}

	table.Render()
}

func (s *Service) handleProofs() error {
	proofs := make(map[string]*types.ProofParam)

	for {
		select {
		case in := <-s.prepareChan:
			prevNodeId := in.Proofs.NodeID
			old, ok := proofs[prevNodeId]
			if ok {
				prevWorkload := old.Proofs.Workload
				newWorkload := in.Proofs.Workload
				in.Proofs.Workload = &types.Workload{
					StartTime:    prevWorkload.StartTime,
					EndTime:      newWorkload.EndTime,
					DownloadSize: prevWorkload.DownloadSize + newWorkload.DownloadSize,
				}
				in.Proofs.Extra.Cost += old.Proofs.Extra.Cost
				in.Proofs.Extra.Count = old.Proofs.Extra.Count + 1
			}
			proofs[prevNodeId] = in
		case <-s.submitChan:
			keyInScheduler := make(map[string]string)
			schedulerGroup := make(map[string][]*types.WorkloadReport)
			for k, param := range proofs {
				_, ok := schedulerGroup[param.SchedulerURL]
				if !ok {
					schedulerGroup[param.SchedulerURL] = make([]*types.WorkloadReport, 0)
				}
				proofs[k].Proofs.Workload.DownloadSpeed = int64(float64(param.Proofs.Workload.DownloadSize) * 1000 / float64(param.Proofs.Extra.Cost))
				keyInScheduler[param.SchedulerURL] = param.SchedulerKey
				schedulerGroup[param.SchedulerURL] = append(schedulerGroup[param.SchedulerURL], proofs[k].Proofs)
			}

			if s.opts.Verbose {
				printReport(proofs)
			}

			var eg errgroup.Group
			for url, paramList := range schedulerGroup {
				if len(paramList) == 0 {
					continue
				}

				eg.Go(func() error {
					key := keyInScheduler[url]
					data, err := encrypt(key, paramList)
					if err != nil {
						return errors.Errorf("encrypting proof failed: %v", err)
					}

					return s.SubmitProofOfWork(url, data)
				})
			}

			if err := eg.Wait(); err != nil {
				log.Errorf("submit proofs: %v", err)
			}

		}
	}
}
