package config

import (
	"net/http"
	"time"
)

type TraversalMode int

const (
	// TraversalModeDFS only supports retrieving CAR files and auto decodes them into the raw file format.
	TraversalModeDFS TraversalMode = iota + 1
	// TraversalModeRange allows you to retrieve files of any type, but it does not decode, the files will be retrieved in their original format.
	// It's important to note that when using `TraversalModeRange` to retrieve a CAR file, the entire file must be downloaded before it can be decoded.
	TraversalModeRange
)

const (
	defaultListenAddr             = ":8863"
	defaultRangeConcurrency       = 10
	defaultRangeSize        int64 = 1 << 20 // 1 MiB
)

// Config is a set of titan SDK options.
type Config struct {
	ListenAddr  string
	Address     string
	Token       string
	HttpClient  *http.Client
	Timeout     time.Duration
	Mode        TraversalMode
	Concurrency int   // for range mode
	RangeSize   int64 // for range mode
}

// Option is a single titan sdk Config.
type Option func(opts *Config)

// DefaultOption returns a default set of options.
func DefaultOption() Config {
	return Config{
		Mode:        TraversalModeDFS,
		ListenAddr:  defaultListenAddr,
		Concurrency: defaultRangeConcurrency,
		RangeSize:   defaultRangeSize,
		Timeout:     30 * time.Second,
	}
}

// AddressOption set titan server address
func AddressOption(address string) Option {
	return func(opts *Config) {
		opts.Address = address
	}
}

// TokenOption set titan server access token
func TokenOption(token string) Option {
	return func(opts *Config) {
		opts.Token = token
	}
}

// Http3ClientOption set HTTP/3 client, ONLY support HTTP/3 protocol
func Http3ClientOption(client *http.Client) Option {
	return func(opts *Config) {
		opts.HttpClient = client
	}
}

// TraversalModeOption set the download file traversal algorithm, default using DFS pre-order walk algorithm for Dag.
func TraversalModeOption(mode TraversalMode) Option {
	return func(opts *Config) {
		opts.Mode = mode
	}
}

// ListenAddressOption set the listen address for titan client, default is :8863
func ListenAddressOption(addr string) Option {
	return func(opts *Config) {
		opts.ListenAddr = addr
	}
}

// RangeConcurrencyOption limits the maximum number of concurrency HTTP requests allowed at the same time.
//
// This option only works when using `TraversalModeRange` to download files.
func RangeConcurrencyOption(concurrency int) Option {
	return func(opts *Config) {
		opts.Concurrency = concurrency
	}
}

// RangeSizeOption specifies the maximum size of each file range that can be downloaded in a single HTTP request.
// Each range of data is read into memory and then written to the output stream, so the amount of memory used is
// directly proportional to the size of rangeSize.
//
// Specifically, the estimated amount of memory used can be calculated as maxConcurrent x rangeSize.
// Keep an eye on memory usage when modifying this value, as setting it too high can result in excessive memory usage and potential out-of-memory errors.
//
// This option only works when using `TraversalModeRange` to download files.
func RangeSizeOption(size int64) Option {
	return func(opts *Config) {
		opts.RangeSize = size
	}
}

// TimeoutOption specifies a time limit for requests made by the http Client.
func TimeoutOption(timeout time.Duration) Option {
	return func(opts *Config) {
		opts.Timeout = timeout
	}
}
