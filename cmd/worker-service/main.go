package main

import (
	"context"
	"flag"
	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
	"os"
	"os/signal"
	"syscall"

	"crypto/tls"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/maodeyi/raft/raft/worker"
	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"net"
	"net/http"
	"strings"
)

var (
	//	db         *gorm.DB
	Id = flag.String("worker id ", "1", "worker id")
)

func main() {
	flag.Parse()
	endpoint := ":50052"
	conn, err := net.Listen("tcp", endpoint)
	if err != nil {
		grpclog.Fatalf("TCP Listen err:%v\n", err)
	}

	// grpc tls server
	grpcServer := grpc.NewServer()
	worker, err := worker.NewWorkerWarpper()
	if err != nil {
		grpclog.Fatalf("NewWorkerWarpper err:%v\n", err)
	}
	err = worker.Init(*Id)
	if err != nil {
		grpclog.Fatalf("worker.Init err:%v\n", err)
	}
	worker.Run()
	api.RegisterStaticFeatureDBWorkerServiceServer(grpcServer, worker)

	// gw server
	ctx := context.Background()
	dopts := []grpc.DialOption{}
	gwmux := runtime.NewServeMux()
	if err = raft_api.RegisterRaftServiceHandlerFromEndpoint(ctx, gwmux, endpoint, dopts); err != nil {
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
	defer func() {
		worker.Close()
		if err := srv.Shutdown(ctx); err != nil {
			grpclog.Errorf("shutdown: ", err)
		}
		grpcServer.GracefulStop()
	}()

	// listen signal
	signals := make(chan os.Signal, 3)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-signals:
			grpclog.Infof("received signal %v", s)
			return
		}
	}
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
