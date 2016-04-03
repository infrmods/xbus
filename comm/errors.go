package comm

import (
	"fmt"
)

const (
	EcodeSystemError     = "system-error"
	EcodeInvalidName     = "invalid-name"
	EcodeInvalidVersion  = "invalid-version"
	EcodeInvalidEndpoint = "invalid-endpoint"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

func NewError(code string, message string) *Error {
	return &Error{Code: code, Message: message}
}

func Errorf(code, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("[%s]: %s", e.Code, e.Message)
}
