package utils

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

func WritePem(path string, perm os.FileMode, typ string, data []byte) error {
	f, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	err = pem.Encode(f, &pem.Block{Type: typ, Bytes: data})
	f.Close()
	return err
}

func WritePrivateKey(path string, perm os.FileMode, key crypto.Signer) error {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return WritePem(path, perm, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(k))
	case *ecdsa.PrivateKey:
		data, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return err
		}
		return WritePem(path, perm, "EC PRIVATE KEY", data)
	default:
		return fmt.Errorf("unknown private key: %v", key)
	}
}

func WriteCert(path string, perm os.FileMode, derBytes []byte) error {
	return WritePem(path, perm, "CERTIFICATE", derBytes)
}
