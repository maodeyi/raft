package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/maodeyi/raft/raft/proxy"

	raft_proxy "gitlab.bj.sensetime.com/mercury/protohub/api/raft-proxy"
)

func main() {
	endpoint := ":50052"
	conn, err := net.Listen("tcp", endpoint)
	if err != nil {
		grpclog.Fatalf("TCP Listen err:%v\n", err)
	}

	// grpc tls server
	grpcServer := grpc.NewServer()
	proxy := proxy.NewProxy()
	err = proxy.Init()
	if err != nil {
		grpclog.Fatalf("proxy Init err:%v\n", err)
	}
	raft_proxy.RegisterRaftProxyServer(grpcServer, proxy)

	// gw server
	ctx := context.Background()
	dopts := []grpc.DialOption{}
	gwmux := runtime.NewServeMux()
	if err = raft_proxy.RegisterRaftProxyHandlerFromEndpoint(ctx, gwmux, endpoint, dopts); err != nil {
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
