package utils

import (
	"fmt"
)

const (
	// EcodeSystemError SYSTEM_ERROR
	EcodeSystemError = "SYSTEM_ERROR"
	// EcodeInvalidParam INVALID_PARAM
	EcodeInvalidParam = "INVALID_PARAM"
	// EcodeMissingParam MISSING_PARAM
	EcodeMissingParam = "MISSING_PARAM"
	// EcodeInvalidName INVALID_NAME
	EcodeInvalidName = "INVALID_NAME"
	// EcodeInvalidService INVALID_SERVICE
	EcodeInvalidService = "INVALID_SERVICE"
	// EcodeInvalidZone INVALID_ZONE
	EcodeInvalidZone = "INVALID_ZONE"
	// EcodeInvalidExt INVALID_EXTENSION
	EcodeInvalidExt = "INVALID_EXTENSION"
	// EcodeInvalidValue INVALID_VALUE
	EcodeInvalidValue = "INVALID_VALUE"
	// EcodeInvalidVersion INVALID_VERSION
	EcodeInvalidVersion = "INVALID_VERSION"
	// EcodeInvalidAddress INVALID_ADDRESS
	EcodeInvalidAddress = "INVALID_ADDRESS"
	// EcodeInvalidEndpoint INVALID_ENDPOINT
	EcodeInvalidEndpoint = "INVALID_ENDPOINT"
	// EcodeDamagedEndpointValue DAMAGED_ENDPOINT_VALUE
	EcodeDamagedEndpointValue = "DAMAGED_ENDPOINT_VALUE"
	// EcodeTooManyAttempts TOO_MANY_ATTEMPTS
	EcodeTooManyAttempts = "TOO_MANY_ATTEMPTS"
	// EcodeNotFound NOT_FOUND
	EcodeNotFound = "NOT_FOUND"
	// EcodeDeadlineExceeded DEADLINE_EXCEEDED
	EcodeDeadlineExceeded = "DEADLINE_EXCEEDED"
	// EcodeCanceled CANCELED
	EcodeCanceled = "CANCELED"
	// EcodeDeleted DELETED
	EcodeDeleted = "DELETED"
	// EcodeChangedServiceDesc CHANGED_SERVICE_DESC
	EcodeChangedServiceDesc = "CHANGED_SERVICE_DESC"
	// EcodeNameDuplicated NAME_DUPLICATED
	EcodeNameDuplicated = "NAME_DUPLICATED"
	// EcodeNotPermitted NOT_PERMITTED
	EcodeNotPermitted = "NOT_PERMITTED"
)

// Error error
type Error struct {
	Code    string   `json:"code"`
	Message string   `json:"message,omitempty"`
	Keys    []string `json:"keys,omitempty"`
}

// NewError new error
func NewError(code string, message string) *Error {
	return &Error{Code: code, Message: message, Keys: nil}
}

// Errorf errorf
func Errorf(code, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Keys: nil}
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("[%s]: %s", e.Code, e.Message)
}

// NewSystemError new system error
func NewSystemError(msg string) *Error {
	return &Error{Code: EcodeSystemError, Message: msg, Keys: nil}
}

// SystemErrorf system errorf
func SystemErrorf(format string, args ...interface{}) *Error {
	return &Error{Code: EcodeSystemError, Message: fmt.Sprintf(format, args...), Keys: nil}
}

// NewNotPermittedError new not permitted error
func NewNotPermittedError(msg string, keys []string) *Error {
	return &Error{
		Code:    EcodeNotPermitted,
		Message: msg,
		Keys:    keys,
	}
}
