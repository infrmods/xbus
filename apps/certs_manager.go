package apps

import (
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/utils"
	"math/big"
	"net"
	"time"
)

type SerialGenerator interface {
	Generate() (*big.Int, error)
}

type CertsConfig struct {
	RootCert string `default:"rootcert.pem"`
	RootKey  string `default:"rootkey.pem"`
}

type CertsCtrl struct {
	rootCert        *x509.Certificate
	rootKey         crypto.Signer
	config          *CertsConfig
	serialGenerator SerialGenerator
}

func NewCertsCtrl(config *CertsConfig, serialGenerator SerialGenerator) (*CertsCtrl, error) {
	certBlock, err := utils.ReadPEM(config.RootCert)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse root cert fail: %v", err)
	}

	keyBlock, err := utils.ReadPEM(config.RootKey)
	if err != nil {
		return nil, err
	}

	var signer crypto.Signer
	switch keyBlock.Type {
	case "RSA PRIVATE KEY":
		if privKey, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes); err == nil {
			signer = privKey
		} else {
			return nil, fmt.Errorf("parse root key(rsa) fail: %v", err)
		}
	case "EC PRIVATE KEY":
		if privKey, err := x509.ParseECPrivateKey(keyBlock.Bytes); err == nil {
			signer = privKey
		} else {
			return nil, fmt.Errorf("parse root key(ec) fail: %v", err)
		}
	case "PRIVATE KEY":
		if privKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes); err == nil {
			signer = privKey.(crypto.Signer)
		} else {
			return nil, fmt.Errorf("parse root key(pkcs8) fail: %v", err)
		}
	default:
		return nil, fmt.Errorf("unsupported key type: %s", keyBlock.Type)
	}

	mgr := &CertsCtrl{rootCert: cert, rootKey: signer,
		config: config, serialGenerator: serialGenerator}
	return mgr, nil
}

func (mgr *CertsCtrl) CertPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(mgr.rootCert)
	return pool
}

func (mgr *CertsCtrl) NewCert(pubkey crypto.PublicKey, subject pkix.Name,
	dnsNames []string, ips []net.IP, days int) ([]byte, error) {
	serialNumber, err := mgr.serialGenerator.Generate()
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      subject,
		DNSNames:     dnsNames,
		IPAddresses:  ips,

		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Duration(days) * time.Hour * 24),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth},
	}

	if data, err := x509.CreateCertificate(rand.Reader, &template, mgr.rootCert,
		pubkey, mgr.rootKey); err == nil {
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data}), nil
	} else {
		glog.Errorf("create cert fail: %v", err)
		return nil, utils.NewSystemError("create cert fail")
	}
}
