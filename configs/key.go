package configs

import (
	"fmt"
	"regexp"

	"github.com/infrmods/xbus/utils"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)

func checkName(name string) error {
	if !rValidName.MatchString(name) {
		return utils.NewError(utils.EcodeInvalidName, "")
	}
	return nil
}

var rValidNamePrefix = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]?$`)

func checkNamePrefix(name string) error {
	if name != "" {
		if !rValidNamePrefix.MatchString(name) {
			return utils.NewError(utils.EcodeInvalidName, "")
		}
	}
	return nil
}

func (ctrl *ConfigCtrl) configKey(name string) string {
	return fmt.Sprintf("%s/%s", ctrl.config.KeyPrefix, name)
}

func (ctrl *ConfigCtrl) endKey() string {
	return utils.RangeEndKey(ctrl.config.KeyPrefix)
}
