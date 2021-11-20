package worker

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"

	"gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"
	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
)

type WorkerWarpper struct {
	workerRaft *WorkerRaft
	b          *backend
	logger     *logrus.Entry
}

func NewWorkerWarpper() (*WorkerWarpper, error) {
	b, err := newBackend()
	if err != nil {
		return nil, err
	}
	return &WorkerWarpper{
		b:      b,
		logger: logrus.StandardLogger().WithField("component", "WorkerWarpper"),
	}, nil
}

func (s *WorkerWarpper) Init() error {
	err := s.workerRaft.Init(s.b)
	if err != nil {
		s.logger.Errorf("WorkerWarpper Init error %v", err)
	}
	s.b.Init(s.workerRaft)
	return nil
}

func (s *WorkerWarpper) Run() {
	s.b.Run()
}

func (s *WorkerWarpper) RequestVote(ctx context.Context, in *api.RequestVoteRequest) (*api.RequestVoteResponse, error) {
	return s.workerRaft.RequestVoteChannel(in), nil
}

func (s *WorkerWarpper) HeartBead(ctx context.Context, in *api.HeartBeadRequest) (*api.HeartBeadResponse, error) {
	return s.workerRaft.HeartBeatChannel(in), nil
}

//
//func (s *WorkerWarpper) Write(ctx context.Context, in *api.WriteRequest) (*api.WriteResponse, error) {
//	resp := &api.WriteResponse{
//		ClusterInfo: &api.ClusterInfo{},
//	}
//	s.workerRaft.mu.Lock()
//	defer s.workerRaft.mu.Unlock()
//
//	if s.workerRaft.state != api.Role_MASTER {
//		return nil, errors.New("not master")
//	}
//	for _, v := range s.workerRaft.clusterInfo {
//		nodeInfo := &api.NodeInfo{
//			Id:         v.Id,
//			Ip:         v.Ip,
//			Port:       v.Port,
//			LastStatus: v.LastStatus,
//			Role:       v.Role,
//		}
//		resp.ClusterInfo.NodeInfo = append(resp.ClusterInfo.NodeInfo, nodeInfo)
//	}
//
//	resp.Role = s.workerRaft.state
//
//	//todo mongo do not need lock
//	if !s.workerRaft.syncdone {
//		return resp, errors.New("sync oplog")
//	}
//	err := s.b.MongoClient.InsertOpLog()
//	if err != nil {
//		return resp, err
//	}
//	s.workerRaft.seq_id++
//
//	return resp, nil
//}
//
//func (s *WorkerWarpper) Read(ctx context.Context, in *api.ReadRequest) (*api.ReadResponse, error) {
//	resp := &api.ReadResponse{ClusterInfo: &api.ClusterInfo{}}
//	return resp, nil
//}

func (s *WorkerWarpper) GetClusterInfo(ctx context.Context, in *api.GetClusterInfoRequest) (*api.GetClusterInfoResponse, error) {
	resp := &api.GetClusterInfoResponse{}
	s.workerRaft.mu.Lock()
	if s.workerRaft.state != api.Role_MASTER {
		return resp, errors.New("no master")
	}
	for _, v := range s.workerRaft.clusterInfo {
		nodeInfo := &api.NodeInfo{
			Id:         v.Id,
			Ip:         v.Ip,
			Port:       v.Port,
			LastStatus: v.LastStatus,
			Role:       v.Role,
		}
		resp.NodeInfo = append(resp.NodeInfo, nodeInfo)
	}
	s.workerRaft.mu.Lock()
	return resp, nil

}

func (s *WorkerWarpper) PingNode(_ context.Context, request *api.PingNodeRequest) (*api.PingNodeResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.PingNodeResponse), nil
}

func (s *WorkerWarpper) IndexNew(_ context.Context, request *api.IndexNewRequest) (*api.IndexNewResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.IndexNewResponse), nil
}

func (s *WorkerWarpper) IndexDel(_ context.Context, request *api.IndexDelRequest) (*api.IndexDelResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.IndexDelResponse), nil
}

func (s *WorkerWarpper) IndexList(_ context.Context, request *api.IndexListRequest) (*api.IndexListResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.IndexListResponse), nil
}

func (s *WorkerWarpper) IndexTrain(_ context.Context, request *api.IndexTrainRequest) (*api.IndexTrainResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.IndexTrainResponse), nil
}

func (s *WorkerWarpper) IndexGet(_ context.Context, request *api.IndexGetRequest) (*api.IndexGetResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*api.IndexGetResponse), nil
}

func (s *WorkerWarpper) FeatureBatchAdd(_ context.Context, request *db.FeatureBatchAddRequest) (*db.FeatureBatchAddResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*db.FeatureBatchAddResponse), nil
}

func (s *WorkerWarpper) FeatureBatchDelete(_ context.Context, request *db.FeatureBatchDeleteRequest) (*db.FeatureBatchDeleteResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*db.FeatureBatchDeleteResponse), nil
}

func (s *WorkerWarpper) FeatureBatchSearch(_ context.Context, request *db.FeatureBatchSearchRequest) (*db.FeatureBatchSearchResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*db.FeatureBatchSearchResponse), nil
}

func (s *WorkerWarpper) FeatureUpdate(_ context.Context, request *db.FeatureUpdateRequest) (*db.FeatureUpdateResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*db.FeatureUpdateResponse), nil
}

func (s *WorkerWarpper) FeatureBatchUpdate(_ context.Context, request *db.FeatureBatchUpdateRequest) (*db.FeatureBatchUpdateResponse, error) {
	resp, err := s.b.sendRequest(request)
	if err != nil {
		return nil, err
	}
	return resp.(*db.FeatureBatchUpdateResponse), nil
}
