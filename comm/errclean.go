package comm

import (
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func CleanErr(err error, sysErrRet, sysErrformat string, args ...interface{}) error {
	grpcCode := grpc.Code(err)
	if grpcCode != codes.Unknown {
		switch grpcCode {
		case codes.NotFound:
			return NewError(EcodeNotFound, "")
		case codes.DeadlineExceeded:
			return NewError(EcodeDeadlineExceeded, "")
		case codes.Canceled:
			return NewError(EcodeCanceled, "")
		}
	}

	switch err {
	case context.DeadlineExceeded:
		return NewError(EcodeDeadlineExceeded, "")
	case context.Canceled:
		return NewError(EcodeCanceled, "")
	}

	glog.Errorf(sysErrformat, args...)
	return NewError(EcodeSystemError, sysErrRet)
}
