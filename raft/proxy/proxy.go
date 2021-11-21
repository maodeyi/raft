package proxy

import (
	"context"
	"gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"

	//"google.golang.org/grpc/codes"
	//"google.golang.org/grpc/status"
	"sync"
	"time"

	"github.com/maodeyi/raft/raft/util"
	"github.com/sirupsen/logrus"
	//"gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"
	"github.com/maodeyi/raft/raft/sniffer"
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
	workerNum   int32
	rrIndex     int32
	clusterInfo map[string]*Node
	sniffer     *sniffer.Sniffer
	Ids         []string
	closeCh     chan struct{}
	startCh     chan bool
	logger      *logrus.Entry
}

//todo
func NewProxy() *Proxy {
	rf := &Proxy{
		sniffer: sniffer.NewSniffer("//dns.***"),
		logger:  logrus.StandardLogger().WithField("component", "Proxy"),
		rrIndex: -1,
		closeCh: make(chan struct{}),
		startCh: make(chan bool),
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

func (s *Proxy) addWorkNode(nodeInfo *api.NodeInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, nodeInfo.Ip+":"+nodeInfo.Port, grpc.WithInsecure())
	//
	//s.mu.Lock()
	//defer s.mu.Unlock()
	node := &Node{
		NodeInfo: &api.NodeInfo{
			Id:   nodeInfo.Id,
			Ip:   nodeInfo.Ip,
			Port: nodeInfo.Port,
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
	s.clusterInfo[node.NodeInfo.Id] = node
	s.Ids = append(s.Ids, node.NodeInfo.Id)
	return err
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

func (s *Proxy) subWokersStauts() {
	addrs, ch := s.sniffer.Subscribe("proxy")
	for k, v := range addrs {
		s.mu.Lock()
		if len(s.clusterInfo) > int(s.workerNum/2) {
			s.startCh <- true
		}

		if len(s.clusterInfo) == int(s.workerNum) {
			s.mu.Unlock()
			break
		}

		s.mu.Unlock()

		nodeInfo, err := util.BuildNodeInfo(k, v)
		if err != nil {
			s.logger.Errorf("BuildNodeInfo error %s %s %v", k, v, err)
			continue
		}
		s.mu.Lock()
		err = s.addWorkNode(nodeInfo)
		s.mu.Unlock()
		if err != nil {
			s.logger.Errorf("proxy inti worker peer error %v", err)
		}

	}

	for true {
		select {
		case <-s.closeCh:
			return
		case msg := <-ch:
			if msg.Begin {
				s.mu.Lock()
				defer s.mu.Unlock()
				_, ok := s.clusterInfo[msg.WorkerID]
				if !ok && len(s.clusterInfo) < int(s.workerNum) {
					nodeInfo, err := util.BuildNodeInfo(msg.WorkerID, msg.Address)
					if err != nil {
						s.logger.Errorf("BuildNodeInfo error %s %s %v", msg.WorkerID, msg.Address, err)
					}
					err = s.addWorkNode(nodeInfo)
					if err != nil {
						s.logger.Errorf("proxy inti worker peer error %v", err)
					}
					if len(s.clusterInfo) == int(s.workerNum/2+1) {
						select {
						case s.startCh <- true:
						default:
						}
					}
					if len(s.clusterInfo) == int(s.workerNum) {
						return
					}
				}
			}
		}
	}
}

func (s *Proxy) Init() error {
	//todo get mongo workernumer
	s.sniffer.Start()
	s.clusterInfo = make(map[string]*Node)
	go s.subWokersStauts()

	s.logger.Infof("waiting for %d worker start...", s.workerNum/2+1)
	select {
	case <-s.startCh:
		s.logger.Infof("%d worker start over, start service", s.workerNum/2+1)
		return nil
	}
}

func (s *Proxy) Close() {
	close(s.closeCh)
	s.sniffer.Stop()
}

func (s *Proxy) roundroubin() *Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	length := len(s.Ids)
	s.rrIndex++
	index := s.rrIndex % int32(length)
	return s.clusterInfo[s.Ids[index]]
}

func (s *Proxy) nodeIsEqual(src *api.NodeInfo, des *api.NodeInfo) bool {
	return src.Ip == des.Ip && src.Port == des.Port && src.Id == des.Id
}

func (s *Proxy) checkClusterInfo(clusterInfo []*api.NodeInfo) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var id string
	//del
	length := len(clusterInfo)
	for i, v := range s.Ids {
		for index, node := range clusterInfo {
			if node.Role == api.Role_MASTER {
				id = node.Id
			}
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
		if v.Role == api.Role_MASTER {
			id = v.Id
		}
		//add
		node, ok := s.clusterInfo[v.Id]
		if !ok {
			err := s.addWorkNode(v)
			if err != nil {
				s.logger.Errorf("checkClusterInfo AddWorkNode error %v", err)
			}
		}

		//update
		if !s.nodeIsEqual(node.NodeInfo, v) {
			err := s.destoryWorkNode(s.clusterInfo[v.Id])
			if err != nil {
				s.logger.Errorf("destoryWorkNode error %v", err)
			}
			err = s.addWorkNode(v)
			if err != nil {
				s.logger.Errorf("checkClusterInfo AddWorkNode error %v", err)
			}
		} else if s.clusterInfo[v.Id].NodeInfo.Role != v.Role {
			s.clusterInfo[v.Id].NodeInfo.Role = v.Role
		}
	}
	return id
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
		node = s.roundroubin().Client
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

func (s *Proxy) cloneNodes(nodes NodesStatus) NodesStatus {
	nodesStaus := NodesStatus{
		clusterInfo: make(map[string]*Node),
		nodesVisted: make(map[string]bool),
	}

	for k, v := range nodes.clusterInfo {
		nodesStaus.clusterInfo[k] = v
		nodesStaus.nodesVisted[k] = nodes.nodesVisted[k]
	}
	return nodesStaus
}

func (s *Proxy) nextUnvisted(nodes NodesStatus) string {
	for k, v := range nodes.nodesVisted {
		if v == false {
			return k
		}
	}
	return ""
}

func (s *Proxy) callMethod(ctx context.Context, client api.StaticFeatureDBWorkerServiceClient, req interface{}) (interface{}, *api.ClusterInfo, error) {
	var resp interface{}
	var clusterInfo *api.ClusterInfo
	var err error
	switch v := req.(type) {
	case *api.IndexNewRequest:
		resp, err = client.IndexNew(ctx, v)
		if resp != nil {
			clusterInfo = resp.(*api.IndexNewResponse).ClusterInfo
		}
	case *api.IndexDelRequest:
		resp, err = client.IndexDel(ctx, v)
		if resp != nil {
			clusterInfo = resp.(*api.IndexDelResponse).ClusterInfo
		}
	//case *api.IndexListRequest:
	//	resp, err = client.IndexList(ctx, v)
	//case *api.IndexGetRequest:
	//	resp, err = client.IndexGet(ctx, v)
	case *db.FeatureBatchAddRequest:
		resp, err = client.FeatureBatchAdd(ctx, v)
		if resp != nil {
			clusterInfo = resp.(*api.FeatureBatchAddResponse).ClusterInfo
		}
	case *db.FeatureBatchDeleteRequest:
		resp, err = client.FeatureBatchDelete(ctx, v)
		if resp != nil {
			clusterInfo = resp.(*api.FeatureBatchDeleteResponse).ClusterInfo
		}
		//case *db.FeatureBatchSearchRequest:
		//	resp, err = client.FeatureSearch(ctx, v)
	}
	return resp, clusterInfo, err
}

func (s *Proxy) tryNode(ctx context.Context, req interface{}) (interface{}, error) {
	nodes, masterIndex := s.getNodesStatus()
MASTER:
	if masterIndex != "" {
		node, ok := nodes.clusterInfo[masterIndex]
		if !ok {
			return nil, util.ErrNotLeader
		}
		resp, clusterInfo, err := s.callMethod(ctx, node.Client, req)

		if err != nil {
			s.logger.Errorf("index %s Client IndexNew error %v", masterIndex, err)
		}
		if clusterInfo != nil {
			ok, unhNodes := util.CheckLegalMaster(clusterInfo.NodeInfo)
			if ok {
				s.checkClusterInfo(clusterInfo.NodeInfo)
				if clusterInfo.Role == api.Role_MASTER {
					return resp, err
				} else {
					nodes.nodesVisted[masterIndex] = true
					masterIndex = s.checkClusterInfo(clusterInfo.NodeInfo)
					goto MASTER
				}
			} else {
				for _, v := range unhNodes {
					nodes.nodesVisted[v] = true
				}
				masterIndex = s.nextUnvisted(nodes)
				goto MASTER
			}
		} else {
			nodes.nodesVisted[masterIndex] = true
			masterIndex = s.nextUnvisted(nodes)
			goto MASTER
		}
	} else {
		return nil, util.ErrNoLeader
	}
}

func (s *Proxy) IndexNew(ctx context.Context, request *api.IndexNewRequest) (*api.IndexNewResponse, error) {
	resp, err := s.tryNode(ctx, request)
	return resp.(*api.IndexNewResponse), err
}

func (s *Proxy) IndexDel(ctx context.Context, request *api.IndexDelRequest) (*api.IndexDelResponse, error) {
	resp, err := s.tryNode(ctx, request)
	return resp.(*api.IndexDelResponse), err
}

func (s *Proxy) IndexList(ctx context.Context, request *api.IndexListRequest) (*api.IndexListResponse, error) {
	for true {
		node := s.roundroubin()
		resp, err := node.Client.IndexList(ctx, request)
		if err != nil {
			s.logger.Errorf("IndexList error", err)
		}
		if resp != nil && resp.ClusterInfo != nil {
			ok, _ := util.CheckLegalMaster(resp.ClusterInfo.NodeInfo)
			if ok {
				s.checkClusterInfo(resp.ClusterInfo.NodeInfo)
				return resp, err
			} else {
				continue
			}
		} else {
			return resp, err
		}
	}
	return nil, nil
}

func (s *Proxy) IndexTrain(_ context.Context, request *api.IndexTrainRequest) (*api.IndexTrainResponse, error) {
	return nil, nil
}

func (s *Proxy) IndexGet(ctx context.Context, request *api.IndexGetRequest) (*api.IndexGetResponse, error) {
	for true {
		node := s.roundroubin()
		resp, err := node.Client.IndexGet(ctx, request)
		if err != nil {
			s.logger.Errorf("IndexGet error", err)
		}
		if resp != nil && resp.ClusterInfo != nil {
			ok, _ := util.CheckLegalMaster(resp.ClusterInfo.NodeInfo)
			if ok {
				s.checkClusterInfo(resp.ClusterInfo.NodeInfo)
				return resp, err
			} else {
				continue
			}
		} else {
			return resp, err
		}
	}
	return nil, nil
}

func (s *Proxy) FeatureBatchAdd(ctx context.Context, request *db.FeatureBatchAddRequest) (*api.FeatureBatchAddResponse, error) {
	resp, err := s.tryNode(ctx, request)
	return resp.(*api.FeatureBatchAddResponse), err
}

func (s *Proxy) FeatureBatchDelete(ctx context.Context, request *db.FeatureBatchDeleteRequest) (*api.FeatureBatchDeleteResponse, error) {
	resp, err := s.tryNode(ctx, request)
	return resp.(*api.FeatureBatchDeleteResponse), err
}

func (s *Proxy) FeatureBatchSearch(ctx context.Context, request *db.FeatureBatchSearchRequest) (*api.FeatureBatchSearchResponse, error) {
	for true {
		node := s.roundroubin()
		resp, err := node.Client.FeatureBatchSearch(ctx, request)
		if err != nil {
			s.logger.Errorf("FeatureBatchSearch error", err)
		}
		if resp != nil && resp.ClusterInfo != nil {
			ok, _ := util.CheckLegalMaster(resp.ClusterInfo.NodeInfo)
			if ok {
				s.checkClusterInfo(resp.ClusterInfo.NodeInfo)
				return resp, err
			} else {
				continue
			}
		} else {
			return resp, err
		}
	}
	return nil, nil
}

func (s *Proxy) FeatureUpdate(ctx context.Context, request *db.FeatureUpdateRequest) (*api.FeatureUpdateResponse, error) {
	//resp, err := s.tryNode(ctx, request)
	//return resp.(*api.FeatureUpdateResponse), err
	return nil, nil
}

func (s *Proxy) FeatureBatchUpdate(ctx context.Context, request *db.FeatureBatchUpdateRequest) (*api.FeatureBatchUpdateResponse, error) {
	//resp, err := s.tryNode(ctx, request)
	//return resp.(*api.FeatureBatchUpdateResponse), err
	return nil, nil
}

func (s *Proxy) RequestVote(ctx context.Context, in *api.RequestVoteRequest) (*api.RequestVoteResponse, error) {
	return nil, nil
}

func (s *Proxy) HeartBead(ctx context.Context, in *api.HeartBeadRequest) (*api.HeartBeadResponse, error) {
	return nil, nil
}
