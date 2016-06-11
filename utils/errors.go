package utils

import (
	"fmt"
)

const (
	EcodeSystemError          = "SYSTEM_ERROR"
	EcodeInvalidParam         = "INVALID_PARAM"
	EcodeMissingParam         = "MISSING_PARAM"
	EcodeInvalidName          = "INVALID_NAME"
	EcodeInvalidVersion       = "INVALID_VERSION"
	EcodeInvalidAddress       = "INVALID_ADDRESS"
	EcodeInvalidEndpoint      = "INVALID_ENDPOINT"
	EcodeDamagedEndpointValue = "DAMAGED_ENDPOINT_VALUE"
	EcodeTooManyAttempts      = "TOO_MANY_ATTEMPTS"
	EcodeNotFound             = "NOT_FOUND"
	EcodeDeadlineExceeded     = "DEADLINE_EXCEEDED"
	EcodeCanceled             = "CANCELED"
	EcodeDeleted              = "DELETED"
	EcodeChangedServiceDesc   = "CHANGED_SERVICE_DESC"
	EcodeNameDuplicated       = "NAME_DUPLICATED"
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

func NewSystemError(msg string) *Error {
	return &Error{Code: EcodeSystemError, Message: msg}
}

func SystemErrorf(format string, args ...interface{}) *Error {
	return &Error{Code: EcodeSystemError, Message: fmt.Sprintf(format, args...)}
}
