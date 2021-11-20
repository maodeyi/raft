package worker

//
//import (
//	"context"
//	"gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"
//	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
//)
//
//type Worker struct {
//	b *backend
//}
//
//func NewWorker() (*Worker, error) {
//	b, err := newBackend()
//	if err != nil {
//		return nil, err
//	}
//	return &Worker{
//		b: b,
//	}, nil
//}
//
//func (w *Worker) PingNode(_ context.Context, request *api.PingNodeRequest) (*api.PingNodeResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.PingNodeResponse), nil
//}
//
//func (w *Worker) IndexNew(_ context.Context, request *api.IndexNewRequest) (*api.IndexNewResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.IndexNewResponse), nil
//}
//
//func (w *Worker) IndexDel(_ context.Context, request *api.IndexDelRequest) (*api.IndexDelResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.IndexDelResponse), nil
//}
//
//func (w *Worker) IndexList(_ context.Context, request *api.IndexListRequest) (*api.IndexListResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.IndexListResponse), nil
//}
//
//func (w *Worker) IndexTrain(_ context.Context, request *api.IndexTrainRequest) (*api.IndexTrainResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.IndexTrainResponse), nil
//}
//
//func (w *Worker) IndexGet(_ context.Context, request *api.IndexGetRequest) (*api.IndexGetResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*api.IndexGetResponse), nil
//}
//
//func (w *Worker) FeatureBatchAdd(_ context.Context, request *db.FeatureBatchAddRequest) (*db.FeatureBatchAddResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*db.FeatureBatchAddResponse), nil
//}
//
//func (w *Worker) FeatureBatchDelete(_ context.Context, request *db.FeatureBatchDeleteRequest) (*db.FeatureBatchDeleteResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*db.FeatureBatchDeleteResponse), nil
//}
//
//func (w *Worker) FeatureBatchSearch(_ context.Context, request *db.FeatureBatchSearchRequest) (*db.FeatureBatchSearchResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*db.FeatureBatchSearchResponse), nil
//}
//
//func (w *Worker) FeatureUpdate(_ context.Context, request *db.FeatureUpdateRequest) (*db.FeatureUpdateResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*db.FeatureUpdateResponse), nil
//}
//
//func (w *Worker) FeatureBatchUpdate(_ context.Context, request *db.FeatureBatchUpdateRequest) (*db.FeatureBatchUpdateResponse, error) {
//	resp, err := w.b.sendRequest(request)
//	if err != nil {
//		return nil, err
//	}
//	return resp.(*db.FeatureBatchUpdateResponse), nil
//}
