package service

import (
	"fmt"
	"time"
)

func (xbus *XBus) etcdKeyPrefix(name, version string) string {
	return fmt.Sprintf("%s/%s/%s/", xbus.config.KeyPrefix, name, version)
}

func (xbus *XBus) etcdKey(name, version, id string) string {
	return fmt.Sprintf("%s/%s/%s/%s", xbus.config.KeyPrefix, name, version, id)
}

func (xbus *XBus) newUniqueEphemeralNode(ttl time.Duration, prefix string, value []byte) (string, error) {
	return "", nil
}
