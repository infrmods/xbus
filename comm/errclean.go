package comm

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc/codes"
	"github.com/golang/glog"
	"golang.org/x/net/context"
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
