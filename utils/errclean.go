package utils

import (
	"context"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// GetErrCode get err code
func GetErrCode(err error) codes.Code {
	var code codes.Code = codes.Unknown
	if etcdErr, ok := err.(rpctypes.EtcdError); ok {
		code = etcdErr.Code()
	} else {
		code = grpc.Code(err)
	}
	return code
}

// CleanErrWithCode clean err with code
func CleanErrWithCode(err error, sysErrRet, sysErrformat string, args ...interface{}) (codes.Code, error) {
	code := GetErrCode(err)
	if code != codes.Unknown {
		switch code {
		case codes.NotFound:
			return code, NewError(EcodeNotFound, "")
		case codes.DeadlineExceeded:
			return code, NewError(EcodeDeadlineExceeded, "")
		case codes.Canceled:
			return code, NewError(EcodeCanceled, "")
		}
	}

	switch err {
	case context.DeadlineExceeded:
		return code, NewError(EcodeDeadlineExceeded, "")
	case context.Canceled:
		return code, NewError(EcodeCanceled, "")
	}

	glog.Errorf(sysErrformat, args...)
	return code, NewError(EcodeSystemError, sysErrRet)
}

// CleanErr clean err
func CleanErr(err error, sysErrRet, sysErrformat string, args ...interface{}) error {
	_, newErr := CleanErrWithCode(err, sysErrRet, sysErrformat, args...)
	return newErr
}
