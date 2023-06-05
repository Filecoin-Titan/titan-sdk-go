package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/pkg/errors"
)

func DecodePublicKey(pubPEM string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pubPEM))
	if block == nil {
		panic("failed to parse PEM block containing the public key")
	}

	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, errors.Errorf("failed to parse DER encoded public key: %v", err.Error())
	}

	return pub, nil
}

func Encrypt(msg []byte, pub *rsa.PublicKey) ([]byte, error) {
	msgLen := len(msg)
	hash := crypto.SHA256.New()
	step := pub.Size() - 2*hash.Size() - 2
	var encryptedBytes []byte

	for start := 0; start < msgLen; start += step {
		finish := start + step
		if finish > msgLen {
			finish = msgLen
		}

		encryptedBlockBytes, err := rsa.EncryptOAEP(hash, rand.Reader, pub, msg[start:finish], nil)
		if err != nil {
			return nil, err
		}

		encryptedBytes = append(encryptedBytes, encryptedBlockBytes...)
	}

	return encryptedBytes, nil
}
