package util

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotLeader     = status.Error(codes.Aborted, "node is not leader")
	ErrNoHalf        = status.Error(codes.Aborted, "cannot connect to over half followers")
	ErrIndexNotFound = status.Error(codes.InvalidArgument, "index not found")
	ErrOplogNotEnd   = status.Error(codes.InvalidArgument, "oplogs not end")
	ErrOplogEof      = status.Error(codes.InvalidArgument, "oplogs eof")
)
