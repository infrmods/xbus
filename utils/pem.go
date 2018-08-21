package utils

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
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

func EncodeToPem(typ string, data []byte) string {
	block := pem.Block{Type: typ, Bytes: data}
	return (string)(pem.EncodeToMemory(&block))
}

func EncodePrivateKeyToPem(key crypto.Signer) (string, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return EncodeToPem("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(k)), nil
	case *ecdsa.PrivateKey:
		data, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return "", err
		}
		return EncodeToPem("EC PRIVATE KEY", data), nil
	default:
		return "", fmt.Errorf("unknown private key: %v", key)
	}
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

func ReadPEM(path string) (*pem.Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pem file(%s) fail: %v", path, err)
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read pem file(%s) fail: %v", path, err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("invalid pem file: %s", path)
	}
	return block, nil
}

func ReadPEMCertificate(path string) (*x509.Certificate, error) {
	if block, err := ReadPEM(path); err == nil {
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("invalid cert(%s) type: %s", path, block.Type)
		}
		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			return cert, nil
		} else {
			return nil, fmt.Errorf("parse cert(%s) fail: %v", path, err)
		}
	} else {
		return nil, err
	}
}
