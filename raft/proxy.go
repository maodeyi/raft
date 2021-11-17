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

type Node struct {
	NodeInfo *raft_api.NodeInfo
	Client   raft_api.RaftServiceClient
	Conn     *grpc.ClientConn
}

type Proxy struct {
	mu          sync.Mutex
	rrIndex     int32
	clusterInfo map[string]*Node
	Ids         []string
}

func NewProxy() *Proxy {
	rf := &Proxy{
		rrIndex: -1,
	}
	return rf
}

func (s *Proxy) destoryWorkNode(node *Node) error {
	return node.Conn.Close()
}

func (s *Proxy) initWorkNode(nodeInfo *raft_api.NodeInfo) (*Node, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "127.0.0.1:8080", grpc.WithInsecure())

	s.mu.Lock()
	defer s.mu.Unlock()
	node := &Node{
		NodeInfo: &raft_api.NodeInfo{
			Id:   "1",
			Ip:   "127.0.0.1",
			Port: "8080",
			Role: raft_api.Role_SLAVE,
		},
	}

	if err != nil {
		node.NodeInfo.LastStatus = false
	} else {
		node.NodeInfo.LastStatus = true
		node.Client = raft_api.NewRaftServiceClient(conn)
		node.Conn = conn
	}
	s.Ids = append(s.Ids, node.NodeInfo.Id)
	return node, err
}

func (s *Proxy) Init() error {
	//todo init clusterInfo
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterInfo = make(map[string]*Node)
	//todo init ip port id default slave
	for _, v := range s.clusterInfo {
		workerNode, err := s.initWorkNode(v.NodeInfo)
		if err != nil {
			DPrintf("proxy inti worker peer error %v", err)
		}

		s.clusterInfo[v.NodeInfo.Id] = workerNode
	}
	return nil
}

func (s *Proxy) roundroubin() raft_api.RaftServiceClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	length := len(s.Ids)
	s.rrIndex++
	index := s.rrIndex % int32(length)
	return s.clusterInfo[s.Ids[index]].Client
}

func (s *Proxy) nodeIsEqual(src *raft_api.NodeInfo, des *raft_api.NodeInfo) bool {
	return src.Ip == des.Ip && src.Port == des.Port && src.Id == des.Id
}

func (s *Proxy) checkClusterInfo(clusterInfo []*raft_api.NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//del
	length := len(clusterInfo)
	for _, v := range s.clusterInfo {
		for index, node := range clusterInfo {
			if v.NodeInfo.Id == node.Id {
				break
			}
			if index == length-1 {
				delete(s.clusterInfo, v.NodeInfo.Id)
			}
		}
	}

	for _, v := range clusterInfo {
		//add
		node, ok := s.clusterInfo[v.Id]
		if !ok {
			node, err := s.initWorkNode(v)
			if err != nil {
				DPrintf("initWorkNode error %v", err)
			}
			s.clusterInfo[v.Id] = node
			continue
		}

		//update
		if !s.nodeIsEqual(node.NodeInfo, v) {
			err := s.destoryWorkNode(s.clusterInfo[v.Id])
			if err != nil {
				DPrintf("destoryWorkNode error %v", err)
			}
			node, err := s.initWorkNode(v)
			if err != nil {
				DPrintf("initWorkNode error %v", err)
			}
			s.clusterInfo[v.Id] = node
		} else if s.clusterInfo[v.Id].NodeInfo.Role != v.Role {
			s.clusterInfo[v.Id].NodeInfo.Role = v.Role
		}
	}
}

func (s *Proxy) Write(ctx context.Context, in *raft_proxy.WriteRequest) (*raft_proxy.WriteResponse, error) {
	workerClient := s.roundroubin()
	req := &raft_api.WriteRequest{}
	workerResp, err := workerClient.Write(ctx, req)
	if err != nil {
		log.Errorf(ctx, "worker write error %v", err)
	}

	if workerResp.Role != raft_api.Role_SLAVE {

	}
	clusterInfo := workerResp.ClusterInfo.NodeInfo
	s.checkClusterInfo(clusterInfo)
	resp := &raft_proxy.WriteResponse{}

	return resp, nil
}

func (s *Proxy) Read(ctx context.Context, in *raft_proxy.ReadRequest) (*raft_proxy.ReadResponse, error) {
	workerClient := s.roundroubin()
	req := &raft_api.ReadRequest{}
	workerResp, err := workerClient.Read(ctx, req)
	if err != nil {
		log.Errorf(ctx, "worker read error %v", err)
	}

	clusterInfo := workerResp.ClusterInfo.NodeInfo
	s.checkClusterInfo(clusterInfo)
	resp := &raft_proxy.ReadResponse{}
	return resp, nil
}

func (s *Proxy) GetClusterInfo(ctx context.Context, in *raft_proxy.GetClusterInfoRequest) (*raft_proxy.GetClusterInfoResponse, error) {
	return nil, nil
}
