package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"math/big"
	"time"

	"github.com/golang/glog"
	"github.com/google/subcommands"
	"github.com/infrmods/xbus/utils"
)

// GenRootCmd gen root key/cert
type GenRootCmd struct {
	Organization string
	CommonName   string
	EcdsaCruve   string
	RSABits      int
	Days         int

	CertFile string
	KeyFile  string
}

// Name cmd name
func (cmd *GenRootCmd) Name() string {
	return "gen-root"
}

// Synopsis cmd synopsis
func (cmd *GenRootCmd) Synopsis() string {
	return "generate root cert/key"
}

// Usage cmd usage
func (cmd *GenRootCmd) Usage() string {
	return ""
}

// SetFlags cmd set flags
func (cmd *GenRootCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.Organization, "org", "XBus", "organization name")
	f.StringVar(&cmd.CommonName, "cn", "XBus CA", "common name")
	f.IntVar(&cmd.RSABits, "rsa-bits", 2048, "rsa key size in bits")
	f.StringVar(&cmd.EcdsaCruve, "ecdsa-curve", "", "ECDSA curve(P224/P256/P384/P521), empty if use RSA")
	f.IntVar(&cmd.Days, "days", 10*365, "cert valid for days")
	f.StringVar(&cmd.CertFile, "cert-out", "rootcert.pem", "cert output file")
	f.StringVar(&cmd.KeyFile, "key-out", "rootkey.pem", "key output file")
}

// Execute cmd execute
func (cmd *GenRootCmd) Execute(_ context.Context, f *flag.FlagSet, v ...interface{}) subcommands.ExitStatus {
	privKey, err := utils.NewPrivateKey(cmd.EcdsaCruve, cmd.RSABits)
	if err != nil {
		glog.Errorf("generate rsa private key fail: %v", err)
		return subcommands.ExitFailure
	}

	if cmd.Days < 1 {
		glog.Errorf("invalid days: %d", cmd.Days)
		return subcommands.ExitFailure
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{cmd.Organization},
			CommonName:   cmd.CommonName,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Duration(cmd.Days) * time.Hour * 24),
		KeyUsage:  x509.KeyUsageCertSign | x509.KeyUsageCRLSign,

		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template,
		privKey.Public(), privKey)
	if err != nil {
		glog.Errorf("create cert fail: %v", err)
		return subcommands.ExitFailure
	}

	if err := utils.WriteCert(cmd.CertFile, 0644, derBytes); err != nil {
		glog.Errorf("write cert fail: %v", err)
		return subcommands.ExitFailure
	}

	if err := utils.WritePrivateKey(cmd.KeyFile, 0600, privKey); err != nil {
		glog.Errorf("write key fail: %v", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
