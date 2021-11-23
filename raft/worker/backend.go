package worker

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/maodeyi/raft/raft/db"
	"github.com/maodeyi/raft/raft/sniffer"
	"github.com/maodeyi/raft/raft/util"
	"github.com/sirupsen/logrus"
	"gitlab.bj.sensetime.com/mercury/protohub/api/commonapis"
	sfd_db "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/db"
	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var snapshotInterval = 5 * time.Minute
var syncInterval = 1 * time.Second
var subcribeInterval = 1 * time.Second

type Worker interface {
	GetSeq() int64
}

type backend struct {
	logger     *logrus.Entry
	syncDoneMu sync.Mutex
	indexes    map[string]*memoryIndex // index id -> memory index

	// cluster fields
	raft     Raft             // raft
	n        int              // worker number in cluster // TODO yore: 这个应该也只跟 sniffer 一起用, 以及通过接口热更新.
	sniffer  *sniffer.Sniffer // TODO yore: sniffer and raft 融合?
	syncDone bool
	// control fields
	request chan *reqWrap
	closeCh chan struct{}

	// master fields
	gen              *generator // generate and save seq id
	snapshotTicker   *time.Ticker
	syncOplogsTicker *time.Ticker
	subOpLogsTicker  *time.Ticker
	MongoClient      *db.MongoClient
}

func newBackend() (*backend, error) {
	// 0. init the world.
	b := &backend{
		logger:     logrus.StandardLogger().WithField("component", "worker"),
		syncDoneMu: sync.Mutex{},
		indexes:    make(map[string]*memoryIndex, 128),
		request:    make(chan *reqWrap),
		closeCh:    make(chan struct{}),
		syncDone:   false,
	}
	return b, nil
}

func (b *backend) Init(raft Raft) {
	b.raft = raft
}

func (b *backend) Run() {
	// 1. recover
	b.recover()

	//todo wether check nodes > 1/2 config workernum
	//select {
	//case <-b.raft.NotifyStart():
	//}

	// 2. start leader electing
	b.raft.LeaderElect() // just start cluster

	// 2. main loop
	go b.mainLoop()

}

func (b *backend) mainLoop() {
	for {
		select {
		case <-b.closeCh:
			return
		case req := <-b.request:
			b.handleRequest(req)
		case <-b.snapshotTicker.C:
			// do snapshot in other thread
			// TODO yore: any data race?
			go b.snapshot()
		case isMaster := <-b.raft.RoleNotify():
			if isMaster == api.Role_MASTER {
				b.syncDoneMu.Lock()
				b.syncDone = false
				b.syncDoneMu.Lock()
				b.logger.Infof("turn to master begin snapshot")
				b.snapshotTicker = time.NewTicker(snapshotInterval)
				b.logger.Infof("turn to master stop subcribe and sync log")
				if b.subOpLogsTicker != nil {
					b.subOpLogsTicker.Stop()
				}
				b.subOpLogsTicker = &time.Ticker{C: make(<-chan time.Time)}
				b.syncOplogsTicker = time.NewTicker(syncInterval)
			} else if isMaster == api.Role_SLAVE {
				b.syncDoneMu.Lock()
				b.syncDone = false
				b.syncDoneMu.Lock()
				b.logger.Infof("turn to slave stop snapshot")
				if b.snapshotTicker != nil {
					b.snapshotTicker.Stop()
				}
				b.snapshotTicker = &time.Ticker{C: make(<-chan time.Time)}
				b.subOpLogsTicker = time.NewTicker(subcribeInterval)
			}
		case <-b.syncOplogsTicker.C:
			err := b.RepalyOplogs()
			if err == util.ErrOplogEof {
				b.syncDoneMu.Lock()
				b.syncDone = true
				b.syncDoneMu.Unlock()
				if b.subOpLogsTicker != nil {
					b.subOpLogsTicker.Stop()
				}
				b.subOpLogsTicker = &time.Ticker{C: make(<-chan time.Time)}
			}
		case <-b.subOpLogsTicker.C:
			_ = b.RepalyOplogs()
		}

	}
}

//race condition if use raft check method
func (b *backend) Healthy(clusterInfo []*api.NodeInfo) bool {
	ok, _ := util.CheckLegalMaster(clusterInfo)
	return ok
}

func (b *backend) isMaster(role api.Role) bool { return role == api.Role_MASTER }

func (b *backend) recover() {
	// 1. load snapshot
	b.loadSnapshot()

	// 2. recover raw-cache for indexes
	// yore: skip

	// 3. replay oplog
	// yore: move to main

	// 4. recover index train(just send retrain request) (just run in master)
	//if b.isMaster() {
	//	// send requests
	//	// yore: move to slave to master
	//}
}

func (b *backend) sendRequest(req interface{}) (interface{}, error) {
	respCh := make(chan *respWrap, 1)
	b.syncDoneMu.Lock()
	if b.syncDone {
		select {
		case b.request <- &reqWrap{req: req, resp: respCh}:
		}
		resp := <-respCh
		return resp.resp, resp.err
	}
	b.syncDoneMu.Unlock()
	return nil, util.ErrSyncIng
}

func (b *backend) handleRequest(req *reqWrap) {
	var resp interface{}
	var err error
	switch v := req.req.(type) {
	case *api.IndexNewRequest:
		resp, err = b.indexNew(v)
	case *api.IndexDelRequest:
		resp, err = b.indexDelete(v)
	case *api.IndexListRequest:
		resp, err = b.indexList(v)
	case *api.IndexGetRequest:
		resp, err = b.indexGet(v)
	case *sfd_db.FeatureBatchAddRequest:
		resp, err = b.featureBatchAdd(v)
	case *sfd_db.FeatureBatchDeleteRequest:
		resp, err = b.featureBatchDel(v)
	case *sfd_db.FeatureBatchSearchRequest:
		resp, err = b.featureSearch(v)
	default:
		req.resp <- &respWrap{err: status.Errorf(codes.Unimplemented, "request type %T not implemented", req.req)}
	}
	req.resp <- &respWrap{resp: resp, err: err}
}

func (b *backend) indexNew(req *api.IndexNewRequest) (*api.IndexNewResponse, error) {
	resp := &api.IndexNewResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if !b.isMaster(clusterInfo.Role) {
		return resp, util.ErrNotLeader
	}

	if !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	//if b.isMaster() && !b.syncDone {
	//	return resp, util.ErrSyncIng
	//}

	if req.GetUuid() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid index id")
	}

	// 1. gen seq-id
	seq := b.gen.GetSeq() + 1

	// 2. create memory index
	mi := newMemoryIndex(req.GetUuid(), seq)

	err := b.MongoClient.InsertOpLog(req.GetUuid(), db.DB_CREATE, []string{}, []string{}, []string{}, []int64{seq})
	if err != nil {
		b.gen.Rollback() // rollback seq
	}

	// 4. save index to index map
	b.indexes[req.GetUuid()] = mi

	resp.Uuid = req.GetUuid()
	resp.Status = sfd_db.Status_INITED
	resp.Size = 0
	resp.CreationTime = timestamppb.Now()
	resp.LastSeqId = seq
	return resp, nil
}

func (b *backend) indexDelete(req *api.IndexDelRequest) (*api.IndexDelResponse, error) {
	resp := &api.IndexDelResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if !b.isMaster(clusterInfo.Role) {
		return resp, util.ErrNotLeader
	}

	if !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	//if b.isMaster() && !b.syncDone {
	//	return resp, util.ErrSyncIng
	//}

	id := req.GetIndexUuid()
	if _, ok := b.indexes[id]; !ok {
		return nil, util.ErrIndexNotFound
	}
	seq := b.gen.GetSeq() + 1
	// 1. TODO yore: write oplog
	err := b.MongoClient.InsertOpLog(id, db.DB_DEL, []string{}, []string{}, []string{}, []int64{seq})
	if err != nil {
		b.gen.Rollback() // rollback seq
	}

	// 2. delete item in index map and release index
	b.indexes[id].release()
	delete(b.indexes, id)

	return resp, nil
}

func (b *backend) indexList(_ *api.IndexListRequest) (*api.IndexListResponse, error) {
	resp := &api.IndexListResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if b.isMaster(clusterInfo.Role) && !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	resp.Indexes = make([]*sfd_db.IndexInfo, 0, len(b.indexes))

	for k, v := range b.indexes {
		resp.Indexes = append(resp.Indexes, &sfd_db.IndexInfo{
			Uuid:      k,
			Size:      v.size(),
			LastSeqId: v.seqID,
		})
	}
	return resp, nil
}

func (b *backend) indexGet(req *api.IndexGetRequest) (*api.IndexGetResponse, error) {
	resp := &api.IndexGetResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if b.isMaster(clusterInfo.Role) && !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	id := req.GetIndexUuid()
	if index, ok := b.indexes[id]; !ok {
		return resp, util.ErrIndexNotFound
	} else {
		resp.Index = &sfd_db.IndexInfo{
			Uuid:      id,
			Size:      index.size(),
			LastSeqId: index.seqID,
		}
		return resp, nil
	}
}

func (b *backend) featureBatchAdd(req *sfd_db.FeatureBatchAddRequest) (*api.FeatureBatchAddResponse, error) {
	resp := &api.FeatureBatchAddResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if !b.isMaster(clusterInfo.Role) {
		return resp, util.ErrNotLeader
	}

	if !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}
	//
	//if b.isMaster() && !b.syncDone {
	//	return resp, util.ErrSyncIng
	//}

	indexID := req.GetColId()
	items := req.GetItems()
	index, ok := b.indexes[indexID]
	if !ok {
		return resp, util.ErrIndexNotFound
	}

	features := make([]*memoryFeature, 0, len(items))
	results := make([]*commonapis.Result, 0, len(items))
	ids := make([]string, 0, len(items))
	var feats []string
	var keys []string
	var seqs []int64
	// 1. parse feature
	for i, v := range items {
		raw := parseFeature(v.GetFeature().GetBlob())
		if raw == nil {
			results = append(results, &commonapis.Result{Code: 3, Error: "invalid feature", Status: 0})
			ids = append(ids, "")
			continue
		}
		feats = append(feats, base64.StdEncoding.EncodeToString(v.GetFeature().GetBlob()))
		keys = append(keys, v.GetKey())
		seqs = append(seqs, b.gen.GetSeq()+int64(1+i))
		results = append(results, &commonapis.Result{Code: 0, Error: "", Status: 0})
		ids = append(ids, genFeatureID(indexID, b.gen.GetSeq()+int64(1+i)))
		features = append(features, &memoryFeature{
			raw: raw,
			key: v.GetKey(),
			seq: b.gen.GetSeq() + int64(1+i),
		})

	}

	// 2. TODO yore: write oplog
	err := b.MongoClient.InsertOpLog(indexID, db.FEATURE_ADD, feats, ids, keys, seqs)
	if err != nil {
		return resp, err
	}

	// 3. save to memory
	index.add(features)
	resp.Results = results
	resp.Ids = ids
	return resp, nil
}

func (b *backend) featureBatchDel(req *sfd_db.FeatureBatchDeleteRequest) (*api.FeatureBatchDeleteResponse, error) {
	resp := &api.FeatureBatchDeleteResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if !b.isMaster(clusterInfo.Role) {
		return resp, util.ErrNotLeader
	}

	if !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	//if b.isMaster() && !b.syncDone {
	//	return resp, util.ErrSyncIng
	//}

	indexID := req.GetColId()
	ids := req.GetIds()

	index, ok := b.indexes[indexID]
	if !ok {
		return resp, util.ErrIndexNotFound
	}

	// 1. parse ids
	results := make([]*commonapis.Result, len(ids))
	seqs := make([]int64, 0, len(ids))
	mapping := make(map[int]int, len(ids)) // seqs idx -> results idx
	for i, v := range ids {
		seq := parseFeatureID(indexID, v)
		if seq == 0 {
			results[i] = &commonapis.Result{
				Code:  int32(codes.InvalidArgument),
				Error: "invalid feature id",
			}
			continue
		}
		mapping[len(seqs)] = i
		results[i] = &commonapis.Result{}
		seqs = append(seqs, seq)
	}

	// 2. TODO yore: write oplog
	if err := b.MongoClient.InsertOpLog(indexID, db.FEATURE_DEL, []string{}, ids, []string{}, seqs); err != nil {
		return nil, err
	}

	// TODO yore: change results when error

	// 3. delete in memory
	index.del(seqs)
	resp.Results = results

	return resp, nil
}

func (b *backend) featureSearch(req *sfd_db.FeatureBatchSearchRequest) (*api.FeatureBatchSearchResponse, error) {
	resp := &api.FeatureBatchSearchResponse{}
	clusterInfo := b.raft.GetClusterInfo()
	resp.ClusterInfo = clusterInfo

	if !b.isMaster(clusterInfo.Role) {
		return resp, util.ErrNotLeader
	}

	if !b.Healthy(clusterInfo.NodeInfo) {
		return resp, util.ErrNoHalf
	}

	//if b.isMaster() && !b.syncDone {
	//	return resp, util.ErrSyncIng
	//}

	indexID := req.GetColId()
	items := req.GetFeatures()

	index, ok := b.indexes[indexID]
	if !ok {
		return nil, util.ErrIndexNotFound
	}

	results := make([]*commonapis.Result, len(items))
	one := make([]*sfd_db.OneFeatureResult, len(items))
	features := make([][]float32, 0, len(items))
	mapping := make(map[int]int, len(items)) // features idx -> results idx
	for i, v := range items {
		f := parseFeature(v.GetBlob())
		if f == nil {
			results[i] = &commonapis.Result{Code: 3, Error: "invalid feature"}
			one[i] = &sfd_db.OneFeatureResult{}
			continue
		}
		results[i] = &commonapis.Result{}
		mapping[len(features)] = i
		features = append(features, f)
	}

	for i, v := range features {
		res := index.search(v, int(req.GetTopK()))
		ret := make([]*sfd_db.SimilarResult, 0, len(res))
		for _, v := range res {
			ret = append(ret, &sfd_db.SimilarResult{
				Item:    &sfd_db.FeatureItem{Id: genFeatureID(indexID, v.seq)},
				Score:   float32(v.score),
				IndexId: indexID,
			})
		}
		one[mapping[i]] = &sfd_db.OneFeatureResult{Results: ret}
	}
	resp.Results = results
	resp.FeatureResults = one
	resp.ColId = indexID
	resp.IsRefined = true
	return resp, nil
}

func (b *backend) RepalyOplogs() error {
	op, err := b.MongoClient.GetOplog(b.gen.GetSeq())
	if op != nil {
		if op.Operation == db.DB_DEL {
			b.indexes[op.FeatureId].release()
			delete(b.indexes, op.FeatureId)
			b.gen.NextSeq()
		} else if op.Operation == db.DB_CREATE {
			mi := newMemoryIndex(op.ColId, op.SeqId)
			b.indexes[op.ColId] = mi
			b.gen.NextSeq()
		} else if op.Operation == db.FEATURE_ADD {
			index, ok := b.indexes[op.ColId]
			if !ok {
				b.logger.Error("replay log db.FEATURE_ADD err")
				return util.ErrIndexNotFound
			}
			blob, _ := base64.StdEncoding.DecodeString(op.Feature)
			raw := parseFeature(blob)
			var features []*memoryFeature
			features = append(features, &memoryFeature{
				raw: raw,
				key: op.Key,
				seq: op.SeqId,
			})

			index.add(features)
			b.gen.NextSeq()
		} else if op.Operation == db.FEATURE_DEL {
			index, ok := b.indexes[op.ColId]
			if !ok {
				b.logger.Error("replay log db.FEATURE_ADD err")
				return util.ErrIndexNotFound
			}
			index.del([]int64{op.SeqId})
		}
	} else if err != nil {
		if err == mongo.ErrNoDocuments {
			return util.ErrOplogEof
		}
		b.logger.Errorf("syncOplogs err %v", err)
		return util.ErrOplogNotEnd
	}
	return err
}

func (b *backend) GetSeq() int64 {
	return b.gen.GetSeq()
}

func (b *backend) stop() {
	close(b.closeCh)
}

type reqWrap struct {
	req  interface{}
	resp chan *respWrap
}

type respWrap struct {
	resp interface{}
	err  error
}

type memoryFeature struct {
	raw []float32
	key string
	seq int64
}

type searchResult struct {
	seq   int64
	score float64
	key   string
}

type memoryIndex struct {
	id       string
	mu       sync.Mutex
	features map[int64]*memoryFeature
	seqID    int64
}

func newMemoryIndex(id string, seq int64) *memoryIndex {
	return &memoryIndex{
		id:       id,
		mu:       sync.Mutex{},
		features: make(map[int64]*memoryFeature, 128),
		seqID:    seq,
	}
}

func (m *memoryIndex) release() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seqID = 0
	m.features = nil
}

func (m *memoryIndex) size() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.features))
}

func (m *memoryIndex) add(features []*memoryFeature) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, v := range features {
		m.features[v.seq] = features[i]
	}
}

func (m *memoryIndex) del(seqs []int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range seqs {
		delete(m.features, v)
	}
}

func (m *memoryIndex) search(raw []float32, topK int) []*searchResult {
	scores := make([]float64, 0, topK)
	seqs := make([]int64, 0, topK)
	for k, v := range m.features {
		score := dot(v.raw, raw)
		seqs, scores = findTopK(k, score, seqs, scores, topK)
	}
	results := make([]*searchResult, 0, topK)
	for i, v := range scores {
		results = append(results, &searchResult{
			seq:   seqs[i],
			score: v,
			key:   m.features[seqs[i]].key,
		})
	}
	return results
}

func findTopK(seq int64, score float64, seqs []int64, scores []float64, topK int) ([]int64, []float64) {
	newSeqs := make([]int64, 0, topK+1)
	newScores := make([]float64, 0, topK+1)
	added := false
	for i, v := range scores {
		if v < score && !added {
			added = true
			newSeqs = append(newSeqs, seq)
			newScores = append(newScores, score)
		}
		newSeqs = append(newSeqs, seqs[i])
		newScores = append(newScores, v)
	}
	if !added {
		newSeqs = append(newSeqs, seq)
		newScores = append(newScores, score)
	}
	if len(newSeqs) > topK {
		newSeqs = newSeqs[:topK]
		newScores = newScores[:topK]
	}
	return seqs, scores
}

func dot(f1, f2 []float32) float64 {
	s := float64(0)
	norm1 := float64(0)
	norm2 := float64(0)
	for i := range f1 {
		xi := float64(f1[i])
		yi := float64(f2[i])
		s += xi * yi
		norm1 += xi * xi
		norm2 += yi * yi
	}
	return s / (math.Sqrt(norm1) * math.Sqrt(norm2))
}

func parseFeature(f []byte) []float32 {
	if len(f) != 256*4 {
		return nil
	}
	raw := make([]float32, 256)
	for i := range raw {
		data := binary.BigEndian.Uint32(f[i*4 : (i+1)*4])
		raw[i] = float32(data)
	}
	return raw
}

func parseFeatureID(indexID, id string) int64 {
	if strings.HasPrefix(id, indexID) {
		hex := strings.TrimPrefix(id, indexID)
		if seq, err := strconv.ParseInt(hex, 16, 64); err == nil {
			return seq
		}
	}
	return 0
}

func genFeatureID(indexID string, seq int64) string {
	return fmt.Sprintf("%s%08x", indexID, seq)
}
