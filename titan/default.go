package titan

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"math/big"
	"net"
	"net/http"
	"time"
)

func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{tlsCert},
		InsecureSkipVerify: true,
	}, nil
}

func defaultTLSConf() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		NextProtos:         []string{http3.NextProtoH3},
	}
}

func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod: time.Second,
	}
}

func defaultHttpClient(conn net.PacketConn) *http.Client {
	tlsConf := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		NextProtos:         []string{http3.NextProtoH3},
	}

	conf := &quic.Config{
		KeepAlivePeriod: time.Second,
	}

	return &http.Client{Transport: &http3.RoundTripper{
		TLSClientConfig: tlsConf,
		QuicConfig:      conf,
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
			address, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				return nil, err
			}
			return quic.DialEarlyContext(ctx, conn, address, "localhost", tlsCfg, cfg)
		},
	}}
}

func newHttpClient(conn quic.EarlyConnection, timeout time.Duration) *http.Client {
	return &http.Client{Transport: &http3.RoundTripper{
		TLSClientConfig: defaultTLSConf(),
		QuicConfig:      defaultQUICConfig(),
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
			return conn, nil
		},
	}, Timeout: timeout}
}

func createConnection(ctx context.Context, conn net.PacketConn, remoteAddr string) (quic.EarlyConnection, error) {
	addr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimout)
	defer cancel()

	return quic.DialEarlyContext(ctx, conn, addr, "localhost", defaultTLSConf(), defaultQUICConfig())
}
