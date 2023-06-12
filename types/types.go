package types

import (
	"fmt"
	"net/http"
	"time"
)

type NATType int

const (
	NATUnknown NATType = iota
	NATOpenInternet
	NATSymmetric
	NATFullCone
	NATRestricted
	NATPortRestricted
	NATUDPBlock
)

func (n NATType) String() string {
	switch n {
	case NATUnknown:
		return "Unknown"
	case NATFullCone:
		return "FullCone"
	case NATRestricted:
		return "Restricted"
	case NATPortRestricted:
		return "PortRestricted"
	case NATSymmetric:
		return "Symmetric"
	case NATOpenInternet:
		return "OpenInternet"
	case NATUDPBlock:
		return "UDPBlock"
	default:
		return ""
	}
}

// Token access download asset
type Token struct {
	ID string
	// CipherText encrypted TokenPayload by public key
	CipherText string
	// Sign signs CipherText by scheduler private key
	Sign string
}

type EdgeDownloadInfo struct {
	Address string
	Tk      *Token
	NodeID  string
	NatType string
}

type EdgeDownloadInfoList struct {
	Infos        []*EdgeDownloadInfo
	SchedulerURL string
	SchedulerKey string
}

type Edge struct {
	Address      string
	Token        *Token
	NodeID       string
	NATType      string
	SchedulerURL string
	SchedulerKey string
}

func (e Edge) GetNATType() NATType {
	switch e.NATType {
	case "NoNAT":
		return NATOpenInternet
	case "SymmetricNAT":
		return NATSymmetric
	case "FullConeNAT":
		return NATFullCone
	case "RestrictedNAT":
		return NATRestricted
	case "PortRestrictedNAT":
		return NATPortRestricted
	default:
		return NATUnknown
	}
}

type Client struct {
	Node       *Edge
	Weight     int
	HttpClient *http.Client
}

type NatPunchReq struct {
	Tk      *Token
	NodeID  string
	Timeout time.Duration
}

func (e Edge) ToNatPunchReq() *NatPunchReq {
	return &NatPunchReq{
		Tk:      e.Token,
		NodeID:  e.NodeID,
		Timeout: 30 * time.Second,
	}
}

type AccessPoint struct {
	AreaID        string
	SchedulerURLs []string
}

type Host struct {
	IP   string
	Port string
}

func (h Host) String() string {
	return fmt.Sprintf("%s:%s", h.IP, h.Port)
}

type FileRange struct {
	Start int64
	End   int64
}

type Workload struct {
	DownloadSpeed int64 // bytes/ms
	DownloadSize  int64
	StartTime     int64
	EndTime       int64
}

type WorkloadReport struct {
	TokenID  string
	ClientID string
	NodeID   string
	Workload *Workload
	Extra    *Extra `json:"-"`
}

type ProofParam struct {
	Proofs       *WorkloadReport
	SchedulerKey string
	SchedulerURL string
}

type Extra struct {
	Cost    int64 //ms
	Count   int64
	Address string
}
