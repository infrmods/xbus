package configs

import (
	"fmt"
	"github.com/infrmods/xbus/comm"
	"regexp"
)

var rValidName = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]{5,}$`)

func checkName(name string) error {
	if !rValidName.MatchString(name) {
		return comm.NewError(comm.EcodeInvalidName, "")
	}
	return nil
}

var rValidNamePrefix = regexp.MustCompile(`(?i)^[a-z][a-z0-9_.-]?$`)

func checkNamePrefix(name string) error {
	if name != "" {
		if !rValidNamePrefix.MatchString(name) {
			return comm.NewError(comm.EcodeInvalidName, "")
		}
	}
	return nil
}

func (configs *Configs) configKey(name string) string {
	return fmt.Sprintf("%s/%s", configs.config.KeyPrefix, name)
}

func (configs *Configs) endKey() string {
	return configs.config.KeyPrefix + "0"
}
