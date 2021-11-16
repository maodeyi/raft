package raft

import (
	"context"

	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
)

type Worker struct {
	worker_raft Worker_Raft
}

func NewWorker() *Worker {
	rf := &Worker{}
	return rf
}

func (s *Worker) Init() error {
	return s.worker_raft.Init()
}

func (s *Worker) Run() {
	s.worker_raft.Run()
}

func (s *Worker) RequestVote(ctx context.Context, in *raft_api.RequestVoteRequest) (*raft_api.RequestVoteResponse, error) {
	return s.worker_raft.RequestVoteChannel(in), nil
}

func (s *Worker) HeartBead(ctx context.Context, in *raft_api.HeartBeadRequest) (*raft_api.HeartBeadResponse, error) {
	return s.worker_raft.HeartBeatChannel(in), nil
}

func (s *Worker) Write(ctx context.Context, in *raft_api.WriteRequest) (*raft_api.WriteResponse, error) {
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

func (s *Worker) Read(ctx context.Context, in *raft_api.ReadRequest) (*raft_api.ReadResponse, error) {
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

func (s *Worker) GetClusterInfo(ctx context.Context, in *raft_api.GetClusterInfoRequest) (*raft_api.GetClusterInfoResponse, error) {
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
