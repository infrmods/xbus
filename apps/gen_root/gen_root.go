package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"github.com/infrmods/xbus/utils"
	"math/big"
	"os"
	"time"
)

var orgName = flag.String("org", "XBus", "organization name")
var commName = flag.String("cn", "XBus CA", "common name")
var rsaBits = flag.Int("rsa-bits", 2048, "rsa key bits")
var years = flag.Int("years", 10, "cert invalid after N years")
var certFile = flag.String("cert-out", "rootcert.pem", "cert output file")
var keyFile = flag.String("key-out", "rootkey.pem", "key output file")

func main() {
	flag.Parse()

	privKey, err := rsa.GenerateKey(rand.Reader, *rsaBits)
	if err != nil {
		fmt.Printf("generate rsa private key fail: %v\n", err)
		os.Exit(-1)
	}

	if *years < 1 {
		fmt.Printf("invalid years: %d\n", *years)
		os.Exit(-1)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{*orgName},
			CommonName:   *commName,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Duration(*years) * time.Hour * 24 * 365),
		KeyUsage:  x509.KeyUsageCertSign | x509.KeyUsageCRLSign,

		BasicConstraintsValid: true,
		IsCA: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template,
		&privKey.PublicKey, privKey)
	if err != nil {
		fmt.Printf("create cert fail: %v\n", err)
		os.Exit(-1)
	}

	if err := utils.WriteCert(*certFile, 0644, derBytes); err != nil {
		fmt.Printf("write cert fail: %v\n", err)
		os.Exit(-1)
	}

	if err := utils.WritePrivateKey(*keyFile, 0600, privKey); err != nil {
		fmt.Printf("write key fail: %v\n", err)
		os.Exit(-1)
	}
}
