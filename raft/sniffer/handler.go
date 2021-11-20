package sniffer

import (
	"context"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/stats"
	"net"
	"strings"
)

const (
	localConnKey  = "local-conn"
	clientAddrKey = "client-addr"
)

type connNotify struct {
	addr    string // connection's remote workers
	isBegin bool   // connection is beginning or ending.
}

// interface validation
var _ stats.Handler = (*statHandler)(nil)

// statHandler is for ClientConn
// to get which remote connection begin or end.
type statHandler struct {
	logger *logrus.Entry
	notify chan<- *connNotify
}

// TagRPC do nothing.
func (s *statHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

// HandleRPC do nothing.
func (s *statHandler) HandleRPC(_ context.Context, _ stats.RPCStats) {}

// TagConn will tag the connection is from local(set a flag) or remote(set an workers).
func (s *statHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if !s.isLocalConn(info) { // connection of other workers
		addr := info.RemoteAddr.String() // must be a string
		ctx = context.WithValue(ctx, clientAddrKey, addr)
	} else {
		ctx = context.WithValue(ctx, localConnKey, struct{}{})
	}
	return ctx
}

// HandleConn will send a notification when remote connections begin or end.
func (s *statHandler) HandleConn(ctx context.Context, cs stats.ConnStats) {
	if cs.IsClient() && !s.isLocalConnCtx(ctx) { // just handle client side and remote connections
		addr, ok := ctx.Value(clientAddrKey).(string)
		if !ok {
			s.logger.Panicf("invalid client workers value: %T", ctx.Value(clientAddrKey))
		}
		switch cs.(type) {
		case *stats.ConnBegin:
			s.notifyConnBegin(addr)
		case *stats.ConnEnd:
			s.notifyConnEnd(addr)
		}
	}
}

func (s *statHandler) notifyConnBegin(addr string) {
	// TODO yore: timeout or metrics?
	s.notify <- &connNotify{addr: addr, isBegin: true}
}

func (s *statHandler) notifyConnEnd(addr string) {
	s.notify <- &connNotify{addr: addr}
}

func (s *statHandler) isLocalConnCtx(ctx context.Context) bool {
	return ctx.Value(localConnKey) != nil
}

func (s *statHandler) isLocalConn(info *stats.ConnTagInfo) bool {
	if info == nil || info.LocalAddr == nil || info.RemoteAddr == nil {
		return false
	}
	return parseIP(info.LocalAddr) == parseIP(info.RemoteAddr)
}

func parseIP(addr net.Addr) string {
	return strings.Split(addr.String(), ":")[0]
}
