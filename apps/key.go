package apps

import (
	"fmt"
	"regexp"
)

func (ctrl *AppCtrl) nodeHoldKey(app, label, key string) string {
	return fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, app, label, key)
}

func (ctrl *AppCtrl) nodeOnlineKey(app, label, key string) string {
	return fmt.Sprintf("%s/%s/%s/node_%s/online", ctrl.config.KeyPrefix, app, label, key)
}

func (ctrl *AppCtrl) nodeKeyPrefix(app, label string) string {
	return fmt.Sprintf("%s/%s/%s/", ctrl.config.KeyPrefix, app, label)
}

var rOnlineNodeKey = regexp.MustCompile(`/node_([^/]+)/online$`)

func (ctrl *AppCtrl) parseOnlineNodeKey(key string) string {
	matches := rOnlineNodeKey.FindStringSubmatch(key)
	if matches != nil {
		return matches[1]
	}
	return ""
}

var rHoldNodeKey = regexp.MustCompile(`/node_([^/]+)$`)

func (ctrl *AppCtrl) parseHoldNodeKey(key string) string {
	matches := rHoldNodeKey.FindStringSubmatch(key)
	if matches != nil {
		return matches[1]
	}
	return ""
}
