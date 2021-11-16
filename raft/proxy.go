package raft

import (
	"context"
	"google.golang.org/appengine/log"
	"google.golang.org/grpc"
	"sync"
	"time"

	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
	raft_proxy "gitlab.bj.sensetime.com/mercury/protohub/api/raft-proxy"
)

type Proxy struct {
	mu           sync.Mutex
	rrIndex      int32
	clusterInfo  raft_api.ClusterInfo
	workerClents []raft_api.RaftServiceClient
}

func NewProxy() *Proxy {
	rf := &Proxy{
		rrIndex: -1,
	}
	return rf
}

func (s *Proxy) initWorkClient(info *raft_api.NodeInfo) (client raft_api.RaftServiceClient, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "127.0.0.1:8080", grpc.WithInsecure())

	s.mu.Lock()
	defer s.mu.Unlock()
	nodeInfo := &raft_api.NodeInfo{
		Id:   "1",
		Ip:   "127.0.0.1",
		Port: "8080",
		Role: raft_api.Role_SLAVE,
	}
	if err != nil {
		nodeInfo.LastStatus = false
		s.clusterInfo.NodeInfo[0] = nodeInfo
		return nil, err
	}
	nodeInfo.LastStatus = true
	s.clusterInfo.NodeInfo[0] = nodeInfo
	return raft_api.NewRaftServiceClient(conn), nil
}

func (s *Proxy) Init() error {
	//todo init clusterInfo
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.clusterInfo.NodeInfo {
		workerClient, err := s.initWorkClient(v)
		if err != nil {
			DPrintf("proxy inti worker peer error %v", err)
		}

		s.workerClents = append(s.workerClents, workerClient)
	}
	return nil
}

func (s *Proxy) roundroubin() raft_api.RaftServiceClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	length := len(s.workerClents)
	s.rrIndex++
	index := s.rrIndex % int32(length)
	return s.workerClents[index]
}

func (s *Proxy) Write(ctx context.Context, in *raft_proxy.WriteRequest) (*raft_proxy.WriteResponse, error) {
	resp := &raft_api.WriteResponse{
		ClusterInfo: &raft_api.ClusterInfo{},
	}
	s.worker_raft.mu.Lock()
	defer s.worker_raft.mu.Unlock()
	for _, v := range s.worker_raft.clusterInfo {
		nodeInfo := &raft_api.NodeInfo{
			Id:         v.Id,
			Ip:         v.Ip,
			Port:       v.Port,
			LastStatus: v.LastStatus,
			Role:       v.Role,
		}
		resp.ClusterInfo.NodeInfo = append(resp.ClusterInfo.NodeInfo, nodeInfo)
	}

	resp.Role = s.worker_raft.state

	//todo mongo do not need lock
	if s.worker_raft.state == raft_api.Role_MASTER {
		err := s.worker_raft.mongoclient.InsertOpLog()
		if err != nil {
			return resp, err
		}
		s.worker_raft.seq_id++
	}
	return resp, nil
}

func (s *Proxy) Read(ctx context.Context, in *raft_proxy.ReadRequest) (*raft_proxy.ReadResponse, error) {
	resp := &raft_api.ReadResponse{ClusterInfo: &raft_api.ClusterInfo{}}

	s.worker_raft.mu.Lock()
	for _, v := range s.worker_raft.clusterInfo {
		nodeInfo := &raft_api.NodeInfo{
			Id:         v.Id,
			Ip:         v.Ip,
			Port:       v.Port,
			LastStatus: v.LastStatus,
			Role:       v.Role,
		}
		resp.ClusterInfo.NodeInfo = append(resp.ClusterInfo.NodeInfo, nodeInfo)
	}

	resp.Role = s.worker_raft.state
	s.worker_raft.mu.Unlock()

	return resp, nil
}

func (s *Proxy) GetClusterInfo(ctx context.Context, in *raft_proxy.GetClusterInfoRequest) (*raft_proxy.GetClusterInfoResponse, error) {
	resp := &raft_api.GetClusterInfoResponse{}
	s.worker_raft.mu.Lock()
	for _, v := range s.worker_raft.clusterInfo {
		nodeInfo := &raft_api.NodeInfo{
			Id:         v.Id,
			Ip:         v.Ip,
			Port:       v.Port,
			LastStatus: v.LastStatus,
			Role:       v.Role,
		}
		resp.NodeInfo = append(resp.NodeInfo, nodeInfo)
	}
	s.worker_raft.mu.Lock()
	return resp, nil

}
