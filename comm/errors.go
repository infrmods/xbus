package comm

import (
	"fmt"
)

const (
	EcodeSystemError          = "system-error"
	EcodeInvalidParam         = "invalid-param"
	EcodeMissingParam         = "missing-param"
	EcodeInvalidName          = "invalid-name"
	EcodeInvalidVersion       = "invalid-version"
	EcodeInvalidAddress       = "invalid-address"
	EcodeInvalidEndpoint      = "invalid-endpoint"
	EcodeDamagedEndpointValue = "damaged-endpoint-value"
	EcodeLoopExceeded         = "loop-exceeded"
	EcodeNotFound             = "not-found"
	EcodeDeadlineExceeded     = "deadline-exceeded"
	EcodeCanceled             = "canceled"
	EcodeDeleted              = "deleted"
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
