package service

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc/codes"
	"github.com/golang/glog"
	"github.com/infrmods/xbus/comm"
	"golang.org/x/net/context"
)

func cleanErr(err error, sysErrRet, sysErrformat string, args ...interface{}) error {
	grpcCode := grpc.Code(err)
	if grpcCode != codes.Unknown {
		switch grpcCode {
		case codes.NotFound:
			return comm.NewError(comm.EcodeNotFound, "")
		case codes.DeadlineExceeded:
			return comm.NewError(comm.EcodeDeadlineExceeded, "")
		case codes.Canceled:
			return comm.NewError(comm.EcodeCanceled, "")
		}
	}

	switch err {
	case context.DeadlineExceeded:
		return comm.NewError(comm.EcodeDeadlineExceeded, "")
	case context.Canceled:
		return comm.NewError(comm.EcodeCanceled, "")
	}

	glog.Infof("err: %#v, de: %#v", err, context.DeadlineExceeded)
	glog.Errorf(sysErrformat, args...)
	return comm.NewError(comm.EcodeSystemError, sysErrRet)
}
