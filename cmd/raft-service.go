package main

import (
	"context"

	"crypto/tls"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"gitlab.bj.sensetime.com/mercury/protohub/api/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"net"
	"net/http"
	"strings"
)

// 定义helloHTTPService并实现约定的接口
type RaftHTTPService struct{}

// HelloHTTPService Hello HTTP服务
var RaftService = RaftHTTPService{}

// SayHello 实现Hello服务接口
func (r RaftHTTPService) RequestVote(ctx context.Context, in *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	resp := new(raft.RequestVoteResponse)
	return resp, nil
}

func (r RaftHTTPService) HeartBead(ctx context.Context, in *raft.HeartBeadRequest) (*raft.HeartBeadResponse, error) {
	resp := new(raft.HeartBeadResponse)
	return resp, nil
}

func main() {
	endpoint := ":50052"
	conn, err := net.Listen("tcp", endpoint)
	if err != nil {
		grpclog.Fatalf("TCP Listen err:%v\n", err)
	}

	// grpc tls server
	grpcServer := grpc.NewServer()
	raft.RegisterRaftServiceServer(grpcServer, RaftService)

	// gw server
	ctx := context.Background()
	dopts := []grpc.DialOption{}
	gwmux := runtime.NewServeMux()
	if err = raft.RegisterRaftServiceHandlerFromEndpoint(ctx, gwmux, endpoint, dopts); err != nil {
		grpclog.Fatalf("Failed to register gw server: %v\n", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", gwmux)

	srv := &http.Server{
		Addr:    endpoint,
		Handler: grpcHandlerFunc(grpcServer, mux),
	}

	grpclog.Infof("gRPC and https listen on: %s\n", endpoint)

	if err = srv.Serve(tls.NewListener(conn, srv.TLSConfig)); err != nil {
		grpclog.Fatal("ListenAndServe: ", err)
	}

	return
}

// grpcHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise. Copied from cockroachdb.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	if otherHandler == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			grpcServer.ServeHTTP(w, r)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}
