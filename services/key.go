package services

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/infrmods/xbus/utils"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)
var rValidService = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}:[a-z0-9][a-z0-9_.-]*$`)
var rValidZone = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_-]{3,}$`)
var rValidExt = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9_-]{3,16}$`)

func checkName(name string) error {
	if !rValidName.MatchString(name) {
		return utils.NewError(utils.EcodeInvalidName, "")
	}
	return nil
}

func checkService(service string) error {
	if !rValidService.MatchString(service) {
		return utils.NewError(utils.EcodeInvalidService, "")
	}
	return nil
}

func checkServiceZone(service, zone string) error {
	if !rValidService.MatchString(service) {
		return utils.NewError(utils.EcodeInvalidService, "")
	}
	if !rValidZone.MatchString(zone) {
		return utils.NewError(utils.EcodeInvalidZone, "")
	}
	return nil
}

func checkExtension(ext string) error {
	if !rValidExt.MatchString(ext) {
		return utils.Errorf(utils.EcodeInvalidExt, "invalid extension: %v", ext)
	}
	return nil
}

var rValidAddress = regexp.MustCompile(`(?i)^[a-z0-9:_.-]+$`)

func (ctrl *ServiceCtrl) checkAddress(addr string) error {
	if addr == "" {
		return utils.NewError(utils.EcodeInvalidEndpoint, "missing address")
	}
	if !rValidAddress.MatchString(addr) {
		return utils.NewError(utils.EcodeInvalidAddress, "")
	}
	if ctrl.config.isAddressBanned(addr) {
		return utils.NewError(utils.EcodeInvalidAddress, "banned")
	}
	return nil
}

func (ctrl *ServiceCtrl) serviceEntryPrefix(name string) string {
	return fmt.Sprintf("%s/%s/", ctrl.config.KeyPrefix, name)
}

const serviceDescNodeKey = "desc"

func (ctrl *ServiceCtrl) serviceDescKey(service, zone string) string {
	return fmt.Sprintf("%s/%s/%s/desc", ctrl.config.KeyPrefix, service, zone)
}

func (ctrl *ServiceCtrl) isServiceDescKey(key string) bool {
	return strings.HasSuffix(key, "/"+serviceDescNodeKey)
}

const serviceKeyNodePrefix = "node_"

func (ctrl *ServiceCtrl) serviceKey(service, zone, addr string) string {
	return fmt.Sprintf("%s/%s/%s/node_%s", ctrl.config.KeyPrefix, service, zone, addr)
}

func (ctrl *ServiceCtrl) extNotifyKey(desc *ServiceDescV1) string {
	return fmt.Sprintf("%s-ext-notifies/%s/%s/%s", ctrl.config.KeyPrefix, desc.Extension, desc.Service, desc.Zone)
}

var rNotifyKey = regexp.MustCompile(`-ext-notifies/[^/]+/([^/]+)/([^/]+)$`)

func (ctrl *ServiceCtrl) parseNotifyKey(key string) (*string, *string) {
	parts := rNotifyKey.FindStringSubmatch(key)
	if parts != nil {
		return &parts[1], &parts[2]
	}
	return nil, nil
}

func (ctrl *ServiceCtrl) extNotifyPrefix(ext string) string {
	return fmt.Sprintf("%s-ext-notifies/%s", ctrl.config.KeyPrefix, ext)
}
