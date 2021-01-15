package utils

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"os"
	"time"

)

// ETCDConfig etcd config
type ETCDConfig struct {
	Endpoints []string      `default:"[\"127.0.0.1:2379\"]"`
	Timeout   time.Duration `default:"5s"`
	CACert    string
}

// NewEtcdClient new etcd client
func (etcd *ETCDConfig) NewEtcdClient() *clientv3.Client {
	var tlsConfig *tls.Config
	if etcd.CACert != "" {
		cert, err := ReadPEMCertificate(etcd.CACert)
		if err != nil {
			glog.Errorf("read etcd's cacertfail: %v", err)
			os.Exit(-1)
		}

		pool := x509.NewCertPool()
		pool.AddCert(cert)
		tlsConfig = &tls.Config{RootCAs: pool}
	}
	etcdConfig := clientv3.Config{
		Endpoints:   etcd.Endpoints,
		DialTimeout: etcd.Timeout,
		TLS:         tlsConfig}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		glog.Errorf("create etcd clientv3 fail: %v", err)
		os.Exit(-1)
	}
	return etcdClient
}
