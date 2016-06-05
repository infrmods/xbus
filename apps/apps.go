package apps

import (
	"database/sql"
	"fmt"
	"github.com/gocomm/dbutil"
	_ "github.com/gocomm/dbutil/dialects/mysql"
	"math/big"
)

var SERIAL_CONFIG_ITEM = "serial_number"

type DBSerialGenerator struct {
	db *sql.DB
}

func (g *DBSerialGenerator) Generate() (*big.Int, error) {
	item, err := GetConfigItem(g.db, SERIAL_CONFIG_ITEM)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 32; i++ {
		n, err := item.GetIntValue()
		if err != nil {
			return nil, err
		}
		item.SetIntValue(n + 1)
		if err := item.UpdateValue(g.db); err == nil {
			return big.NewInt(n + 1), nil
		} else if err == dbutil.ZeroEffected {
			continue
		} else {
			return nil, err
		}
	}
	return nil, fmt.Errorf("update loop exceeded")
}

type Config struct {
	DB struct {
		Driver string
		Source string
	}
	Cert CertsConfig
}

type AppCtrl struct {
	config *Config
	db     *sql.DB
	certs  *CertsCtrl
}

func NewAppCtrl(config *Config) (*AppCtrl, error) {
	db, err := sql.Open(config.DB.Driver, config.DB.Source)
	if err != nil {
		return nil, fmt.Errorf("open db fail: %v", err)
	}
	certs, err := NewCertsCtrl(&config.Cert, &DBSerialGenerator{db})
	if err != nil {
		return nil, err
	}
	return &AppCtrl{config: config, db: db, certs: certs}, nil
}
