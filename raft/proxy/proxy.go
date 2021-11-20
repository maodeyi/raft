package proxy

import (
	"context"
	"sync"
	"time"

	"github.com/maodeyi/raft/raft/util"
	"github.com/sirupsen/logrus"
	"gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"
	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
	"google.golang.org/grpc"
)

type Node struct {
	NodeInfo      *api.NodeInfo
	Client        api.StaticFeatureDBWorkerServiceClient
	Conn          *grpc.ClientConn
	ClusterStatus bool
}

type NodesStatus struct {
	clusterInfo map[string]*Node
	nodesVisted map[string]bool
}

type Proxy struct {
	mu          sync.Mutex
	rrIndex     int32
	clusterInfo map[string]*Node
	Ids         []string
	logger      *logrus.Entry
}

func NewProxy() *Proxy {
	rf := &Proxy{
		logger:  logrus.StandardLogger().WithField("component", "Proxy"),
		rrIndex: -1,
	}
	return rf
}

func (s *Proxy) destoryWorkNode(node *Node) error {
	index := -1
	for i, v := range s.Ids {
		if v == node.NodeInfo.Id {
			index = i
		}
	}

	s.Ids = append(s.Ids[:0], s.Ids[index:]...)
	return node.Conn.Close()
}

func (s *Proxy) initWorkNode(nodeInfo *api.NodeInfo) (*Node, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "127.0.0.1:8080", grpc.WithInsecure())

	s.mu.Lock()
	defer s.mu.Unlock()
	node := &Node{
		NodeInfo: &api.NodeInfo{
			Id:   "1",
			Ip:   "127.0.0.1",
			Port: "8080",
			Role: api.Role_SLAVE,
		},
		ClusterStatus: true,
	}

	if err != nil {
		node.NodeInfo.LastStatus = false
	} else {
		node.NodeInfo.LastStatus = true
		node.Client = api.NewStaticFeatureDBWorkerServiceClient(conn)
		node.Conn = conn
	}
	s.Ids = append(s.Ids, node.NodeInfo.Id)
	return node, err
}

func (s *Proxy) getMaster() api.StaticFeatureDBWorkerServiceClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.clusterInfo {
		if v.NodeInfo.Role == api.Role_MASTER {
			return v.Client
		}
	}
	return nil
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
			s.logger.Errorf("proxy inti worker peer error %v", err)
		}

		s.clusterInfo[v.NodeInfo.Id] = workerNode
	}
	return nil
}

func (s *Proxy) roundroubin() api.StaticFeatureDBWorkerServiceClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	length := len(s.Ids)
	s.rrIndex++
	index := s.rrIndex % int32(length)
	return s.clusterInfo[s.Ids[index]].Client
}

func (s *Proxy) nodeIsEqual(src *api.NodeInfo, des *api.NodeInfo) bool {
	return src.Ip == des.Ip && src.Port == des.Port && src.Id == des.Id
}

func (s *Proxy) checkClusterInfo(clusterInfo []*api.NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//del
	length := len(clusterInfo)
	for i, v := range s.Ids {
		for index, node := range clusterInfo {
			if v == node.Id {
				break
			}
			if index == length-1 {
				delete(s.clusterInfo, v)
				s.Ids = append(s.Ids[:0], s.Ids[i:]...)
			}
		}
	}

	for _, v := range clusterInfo {
		//add
		node, ok := s.clusterInfo[v.Id]
		if !ok {
			node, err := s.initWorkNode(v)
			if err != nil {
				s.logger.Errorf("initWorkNode error %v", err)
			}
			s.clusterInfo[v.Id] = node
			continue
		}

		//update
		if !s.nodeIsEqual(node.NodeInfo, v) {
			err := s.destoryWorkNode(s.clusterInfo[v.Id])
			if err != nil {
				s.logger.Errorf("destoryWorkNode error %v", err)
			}
			node, err := s.initWorkNode(v)
			if err != nil {
				s.logger.Errorf("initWorkNode error %v", err)
			}
			s.clusterInfo[v.Id] = node
		} else if s.clusterInfo[v.Id].NodeInfo.Role != v.Role {
			s.clusterInfo[v.Id].NodeInfo.Role = v.Role
		}
	}
}

func (s *Proxy) checkLegalMaster(clusterInfo []*api.NodeInfo) bool {
	number := len(clusterInfo)
	var healthNumber int32
	for _, v := range clusterInfo {
		if v.LastStatus {
			healthNumber++
		}
	}

	if healthNumber > int32(number/2+1) {
		return true
	}
	return false
}

func (s *Proxy) GetClusterInfo(ctx context.Context, in *api.GetClusterInfoRequest) (*api.GetClusterInfoResponse, error) {
	return nil, nil
}

func (s *Proxy) PingNode(_ context.Context, request *api.PingNodeRequest) (*api.PingNodeResponse, error) {
	return nil, nil
}

func (s *Proxy) tryMaster() api.StaticFeatureDBWorkerServiceClient {
	node := s.getMaster()
	if node == nil {
		node = s.roundroubin()
	}
	return node
}

func (s *Proxy) getNodesStatus() (NodesStatus, string) {
	s.mu.Lock()
	nodes := NodesStatus{
		clusterInfo: util.CloneClusterInfo(s.clusterInfo),
		nodesVisted: make(map[string]bool),
	}
	s.mu.Unlock()

	var id string

	for k, _ := range nodes.clusterInfo {
		nodes.nodesVisted[k] = false
		if nodes.clusterInfo[k].NodeInfo.Role == api.Role_MASTER {
			id = k
		}
	}
	return nodes, id
}

func (s *Proxy) IndexNew(ctx context.Context, request *api.IndexNewRequest) (*api.IndexNewResponse, error) {
	nodes, masterIndex := s.getNodesStatus()
	if masterIndex != "" {
		node, _ := nodes.clusterInfo[masterIndex]
		workerResp, err := node.Client.IndexNew(ctx, request)
		if workerResp != nil {
			ok, unhNodes := util.CheckLegalMaster(workerResp.ClusterInfo.NodeInfo)
			if ok {
				s.checkClusterInfo(workerResp.ClusterInfo.NodeInfo)
				if workerResp.Role == api.Role_MASTER {
					return workerResp, err
				}
			} else {
				for _, v := range unhNodes {
					nodes.nodesVisted[v] = true
				}

			}
		}

		if err != nil {
			s.logger.Errorf("Client IndexNew error %v", err)
		}
		nodes.nodesVisted[masterIndex] = true
	} else {

	}

	workerResp.ClusterInfo
	return nil, util.ErrNotLeader
}

func (s *Proxy) IndexDel(ctx context.Context, request *api.IndexDelRequest) (*api.IndexDelResponse, error) {
	master := s.getMaster()
	if master != nil {
		return master.Client.IndexDel(ctx, request)
	}
	return nil, util.ErrNotLeader
}

func (s *Proxy) IndexList(ctx context.Context, request *api.IndexListRequest) (*api.IndexListResponse, error) {
	node := s.roundroubin()
	return node.IndexList(ctx, request)
}

func (s *Proxy) IndexTrain(_ context.Context, request *api.IndexTrainRequest) (*api.IndexTrainResponse, error) {
	return nil, nil
}

func (s *Proxy) IndexGet(ctx context.Context, request *api.IndexGetRequest) (*api.IndexGetResponse, error) {
	node := s.roundroubin()
	return node.IndexGet(ctx, request)
}

func (s *Proxy) FeatureBatchAdd(ctx context.Context, request *db.FeatureBatchAddRequest) (*db.FeatureBatchAddResponse, error) {
	resp := &proxy_api.FeatureBatchAddResponse{}
	master := s.getMaster()
	if master != nil {
		workerResp, err := master.Client.FeatureBatchAdd(ctx, request)
		if workerResp != nil {
			resp.Results = workerResp.Results
			resp.Ids = workerResp.Ids
		}
		return resp, err
	}
	return nil, util.ErrNotLeader
}

func (s *Proxy) FeatureBatchDelete(ctx context.Context, request *db.FeatureBatchDeleteRequest) (*db.FeatureBatchDeleteResponse, error) {
	resp := &proxy_api.FeatureBatchDeleteResponse{}
	master := s.getMaster()
	if master != nil {
		workerResp, err := master.Client.FeatureBatchDelete(ctx, request)
		if workerResp != nil {
			resp.Results = workerResp.Results
		}
		return resp, err
	}
	return nil, util.ErrNotLeader
}

func (s *Proxy) FeatureBatchSearch(ctx context.Context, request *db.FeatureBatchSearchRequest) (*db.FeatureBatchSearchResponse, error) {
	resp := &proxy_api.FeatureBatchSearchResponse{}
	node := s.roundroubin()
	workerResp, err := node.FeatureBatchSearch(ctx, request)
	if workerResp != nil {
		resp.ColId = workerResp.ColId
		resp.FeatureResults = workerResp.FeatureResults
		resp.IsRefined = workerResp.IsRefined
		resp.Results = workerResp.Results
	}
	return resp, err

}

func (s *Proxy) FeatureUpdate(ctx context.Context, request *db.FeatureUpdateRequest) (*db.FeatureUpdateResponse, error) {
	resp := &proxy_api.FeatureUpdateResponse{}
	master := s.getMaster()
	if master != nil {
		workerResp, err := master.Client.FeatureUpdate(ctx, request)
		if workerResp != nil {
			//to do check master
			return resp, err
		}
		return resp, err
	}
	return nil, util.ErrNotLeader
}

func (s *Proxy) FeatureBatchUpdate(ctx context.Context, request *db.FeatureBatchUpdateRequest) (*db.FeatureBatchUpdateResponse, error) {
	resp := &proxy_api.FeatureBatchUpdateResponse{}
	master := s.getMaster()
	if master != nil {
		workerResp, err := master.Client.FeatureBatchUpdate(ctx, request)
		if workerResp != nil {
			resp.Results = workerResp.Results
		}
		return resp, err
	}
	return nil, util.ErrNotLeader
}

func (s *Proxy) RequestVote(ctx context.Context, in *api.RequestVoteRequest) (*api.RequestVoteResponse, error) {
	return nil, nil
}

func (s *Proxy) HeartBead(ctx context.Context, in *api.HeartBeadRequest) (*api.HeartBeadResponse, error) {
	return nil, nil
}
