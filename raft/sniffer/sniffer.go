package sniffer

import (
	"context"
	"github.com/sirupsen/logrus"
	"gitlab.bj.sensetime.com/mercury/protohub/api/raft"
	"google.golang.org/grpc"
	"strings"
	"sync"
	"time"
)

type Msg struct {
	WorkerID string
	Address  string // ip:port
	Begin    bool   // connection begin or end
}

type config struct {
	notifyEnd     bool          // send notification when connection end, default is false.
	retryInterval time.Duration // interval of dialing target when failed, default is 1 second.
	dialTimeout   time.Duration // timeout with dialing to target and worker address, default is 5 seconds.
}

func defaultConfig() *config {
	return &config{
		notifyEnd:     false,
		retryInterval: time.Second,
		dialTimeout:   5 * time.Second,
	}
}

type Option func(*config)

// WithConnEndNotify set the option that sniffer will
// send a msg when a worker connection end.
func WithConnEndNotify() Option {
	return func(c *config) {
		c.notifyEnd = true
	}
}

// WithRetryInterval set the interval duration of dialing target.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *config) {
		c.retryInterval = interval
	}
}

// WithDialTimeout set the timeout duration of dialing target or worker address.
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.dialTimeout = timeout
	}
}

type Sniffer struct {
	logger *logrus.Entry
	target string  // a headless service, like `dns:///my-service.default.svc:7788`
	config *config // configs for Sniffer

	mu      sync.Mutex           // lock for workers and sub.
	workers map[string]string    // address -> workerID.
	sub     map[string]chan *Msg // subscriber name -> notify channel.

	notifyCh chan *connNotify // channel to receive connection stat notification.

	startOnce sync.Once
	stopOnce  sync.Once

	closeCh chan struct{} // main loop control
	wg      sync.WaitGroup
}

// NewSniffer create a sniffer of the target workers to
// get all connections in an grpc connection.
// Each connection presents a worker.
func NewSniffer(target string, options ...Option) *Sniffer {
	s := &Sniffer{
		logger: logrus.StandardLogger().WithField("component", "sniffer"),
		target: target,
		config: defaultConfig(),

		mu:      sync.Mutex{},
		workers: make(map[string]string, 64),
		sub:     make(map[string]chan *Msg, 64),

		notifyCh: make(chan *connNotify, 64),

		startOnce: sync.Once{},
		stopOnce:  sync.Once{},

		closeCh: make(chan struct{}),
		wg:      sync.WaitGroup{},
	}
	for _, v := range options {
		v(s.config)
	}
	return s
}

// Start run the main loop.
func (s *Sniffer) Start() {
	start := func() {
		s.wg.Add(1) // Start goroutine
		defer s.wg.Done()

		// process connection notification
		s.wg.Add(1)
		go s.processNotification()

		var conn *grpc.ClientConn
		var err error
		defer func() {
			if conn != nil {
				_ = conn.Close()
			}
		}()
		s.logger.Infof("sniffer for `%s` is starting", s.target)
		sigCh := make(<-chan time.Time)
		for {
			sig := sigCh
			conn, err = s.dialTarget()
			if err != nil {
				s.logger.Warningf("dial target failed: %v", err)
				sig = time.NewTimer(s.config.retryInterval * time.Second).C
			}
			select {
			case <-s.closeCh:
				return
			case <-sig: // never receive when dial succeeded.
			}
		}
	}
	s.startOnce.Do(func() {
		go start()
	})
}

// Stop will stop all goroutine and close subscribe channels.
func (s *Sniffer) Stop() {
	s.stopOnce.Do(func() {
		s.logger.Infof("close sniffer")
		close(s.closeCh)
		s.wg.Wait()
		// close all subscribe channels
		for _, v := range s.sub {
			close(v)
		}
		s.logger.Infof("sniffer is closed")
	})
}

// Subscribe return current worker info and a notification channel.
// worker info is workerID to address.
func (s *Sniffer) Subscribe(name string) (map[string]string, <-chan *Msg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan *Msg, 64)
	s.sub[name] = ch
	workers := make(map[string]string, len(s.workers))
	for addr, workerID := range s.workers {
		if workerID != "" { // valid worker
			workers[workerID] = addr
		}
	}
	return workers, ch
}

// Unsubscribe remove the notification channel of worker info.
func (s *Sniffer) Unsubscribe(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sub, name)
}

// GetWorkerNum return connecting worker number.
func (s *Sniffer) GetWorkerNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.workers {
		if v != "" {
			n++
		}
	}
	return n
}

// processNotification is the main loop to process worker join or leave.
func (s *Sniffer) processNotification() {
	defer s.wg.Done()
	defer close(s.notifyCh)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.processAddress()
		case msg := <-s.notifyCh: // should not close by other goroutine
			var workerID string
			var err error
			if msg.isBegin {
				// get worker id
				workerID, err = s.getWorkerID(msg.addr)
				if err != nil {
					s.logger.Warningf("get worker id for workers %v failed: %v", msg.addr, err)
				}
			}
			s.updateAndPost(workerID, msg.addr, msg.isBegin)
		}
	}
}

// processAddress find workerID of unknown address.
func (s *Sniffer) processAddress() {
	for addr, wid := range s.workers {
		if wid == "" { // workerID is empty
			workerID, err := s.getWorkerID(addr)
			if err != nil || workerID == "" {
				continue // just wait for next tick
			}
			s.updateAndPost(addr, workerID, true)
		}
	}
}

// updateAndPost will update worker info in memory and send notification to subscribers.
func (s *Sniffer) updateAndPost(addr, workerID string, begin bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// update cache
	if begin {
		s.workers[addr] = workerID
	} else {
		// worker ID must be empty when connection closed
		workerID = s.workers[addr]
		delete(s.workers, addr)
	}
	// send notification when begin or notifyEnd.
	if workerID != "" && (begin || s.config.notifyEnd) {
		for k, v := range s.sub {
			select {
			case v <- &Msg{WorkerID: workerID, Address: addr, Begin: begin}:
			default:
				s.logger.Warningf("send notification to '%v' failed, msg is: worker=%v, addr=%v, begin=%v",
					k, workerID, addr, begin)
			}
		}
	}
}

// getWorkerID get workerID by address through gRPC request.
func (s *Sniffer) getWorkerID(address string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, s.dialOptions()...)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	resp, err := raft.NewRaftServiceClient(conn).GetClusterInfo(ctx, &raft.GetClusterInfoRequest{})
	if err != nil {
		return "", err
	}
	ip := strings.Split(address, ":")[0]
	for _, v := range resp.GetNodeInfo() {
		if v.GetIp() == ip {
			return v.GetId(), nil
		}
	}
	return "", nil
}

// dialTarget create a gRPC connection to target.
func (s *Sniffer) dialTarget() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, s.target, s.dialOptions()...)
}

// dialOptions is gRPC options for dialing target or worker address.
func (s *Sniffer) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStatsHandler(&statHandler{
			logger: s.logger,
			notify: s.notifyCh,
		}),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	}
}
