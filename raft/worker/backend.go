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
	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
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

type backend struct {
	logger  *logrus.Entry
	mu      sync.RWMutex            // lock for indexes
	indexes map[string]*memoryIndex // index id -> memory index

	// cluster fields
	raft    *WorkerRaft      // raft
	n       int              // worker number in cluster // TODO yore: 这个应该也只跟 sniffer 一起用, 以及通过接口热更新.
	sniffer *sniffer.Sniffer // TODO yore: sniffer and raft 融合?

	// control fields
	request chan *reqWrap
	closeCh chan struct{}

	// master fields
	gen            *generator // generate and save seq id
	snapshotTicker *time.Ticker
	MongoClient    *db.MongoClient
}

func newBackend() (*backend, error) {
	// 0. init the world.
	b := &backend{
		logger:  logrus.StandardLogger().WithField("component", "worker"),
		mu:      sync.RWMutex{},
		indexes: make(map[string]*memoryIndex, 128),

		//raft:    Raft(nil),              // TODO yore: impl
		n:       3,                      // TODO yore: read from config file
		sniffer: sniffer.NewSniffer(""), // TODO yore: impl

		request: make(chan *reqWrap),
		closeCh: make(chan struct{}),
	}
	return b, nil
}

func (b *backend) Init(raft *WorkerRaft) {
	b.raft = raft
}

func (b *backend) Run() {
	// 1. recover
	b.recover()

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
		}
	}
}

func (b *backend) subscribeOpLogs() {
	go func() {
		for true {
			select {
			//when raft change into follower from master receive from subscribeOplogsCh
			case isMaster := <-b.raft.isMasterCh:
				if isMaster == raft_api.Role_MASTER {
					b.logger.Infof("turn to master stop subcribe and sync log")
					break
				}
			default:
			}
			time.Sleep(300 * time.Millisecond)
			b.SyncOplos()
		}
	}()
}

func (b *backend) Snapshot() {
	for true {
		select {
		case isMaster := <-b.raft.isMasterCh:
			if isMaster == raft_api.Role_MASTER {
				b.logger.Infof("turn to master begin snapshot")
				b.snapshotTicker = time.NewTicker(snapshotInterval)
			} else if isMaster == raft_api.Role_SLAVE {
				b.logger.Infof("turn to slave stop snapshot")
				if b.snapshotTicker != nil {
					b.snapshotTicker.Stop()
				}
				b.snapshotTicker = &time.Ticker{C: make(<-chan time.Time)}
			}
		default:
		}
	}
}

func (b *backend) handleRoleChange(old, new raft_api.Role) {
	if old == new {
		return
	}

	if new == raft_api.Role_SLAVE {
		// 1. TODO yore: start oplog subscriber

		// 2. stop snapshot
		if b.snapshotTicker != nil {
			b.snapshotTicker.Stop()
		}
		b.snapshotTicker = &time.Ticker{C: make(<-chan time.Time)}

		// 3. TODO yore: stop index trainer

	}
	if new == raft_api.Role_MASTER {
		// 1. TODO yore: stop oplog subscriber

		// 2. start snapshot
		b.snapshotTicker = time.NewTicker(snapshotInterval)

		// 3. TODO yore: start index trainer
	}
}

func (b *backend) isMaster() bool { return b.raft.IsMaster() }

// TODO yore: change to use raft instead of sniffer.
func (b *backend) writePreflight() error {
	if !b.isMaster() {
		return util.ErrNotLeader
	}
	n := b.sniffer.GetWorkerNum()
	if !(n > b.n/2) {
		b.logger.Debugf("cannot connect to over half workers: %v of %v", n, b.n)
		return util.ErrNoHalf
	}
	return nil
}

func (b *backend) recover() {
	// 1. load snapshot
	b.loadSnapshot()

	// 2. recover raw-cache for indexes
	// yore: skip

	// 3. replay oplog
	// yore: move to main loop

	// 4. recover index train(just send retrain request) (just run in master)
	if b.isMaster() {
		// send requests
		// yore: move to slave to master
	}
}

func (b *backend) sendRequest(req interface{}) (interface{}, error) {
	respCh := make(chan *respWrap, 1)
	select {
	case b.request <- &reqWrap{req: req, resp: respCh}:
	}
	resp := <-respCh
	return resp.resp, resp.err
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
	if !b.isMaster() {
		return nil, util.ErrNotLeader
	}
	if req.GetUuid() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid index id")
	}

	// 1. gen seq-id
	seq := b.gen.GetSeq() + 1

	// 2. create memory index
	mi := newMemoryIndex(req.GetUuid(), seq)

	if err := b.writePreflight(); err != nil {
		return nil, err
	}
	// 3. TODO yore: write oplog
	var err error // write oplog error
	if err != nil {
		b.gen.Rollback() // rollback seq
	}

	// 4. save index to index map
	b.mu.Lock()
	b.indexes[req.GetUuid()] = mi
	b.mu.Unlock()

	return &api.IndexNewResponse{
		Uuid:         req.GetUuid(),
		Status:       sfd_db.Status_INITED,
		Size:         0,
		CreationTime: timestamppb.Now(),
		LastSeqId:    seq,
	}, nil
}

func (b *backend) indexDelete(req *api.IndexDelRequest) (*api.IndexDelResponse, error) {
	if !b.isMaster() {
		return nil, util.ErrNotLeader
	}
	id := req.GetIndexUuid()
	if _, ok := b.indexes[id]; !ok {
		return nil, util.ErrIndexNotFound
	}
	// 1. TODO yore: write oplog
	var err error // write oplog error
	if err != nil {
		b.gen.Rollback() // rollback seq
	}

	// 2. delete item in index map and release index
	b.mu.Lock()
	b.indexes[id].release()
	delete(b.indexes, id)
	b.mu.Unlock()

	return &api.IndexDelResponse{}, nil
}

func (b *backend) indexList(_ *api.IndexListRequest) (*api.IndexListResponse, error) {
	resp := api.IndexListResponse{
		Indexes: make([]*sfd_db.IndexInfo, 0, len(b.indexes)),
	}
	for k, v := range b.indexes {
		resp.Indexes = append(resp.Indexes, &sfd_db.IndexInfo{
			Uuid:      k,
			Size:      v.size(),
			LastSeqId: v.seqID,
		})
	}
	return &resp, nil
}

func (b *backend) indexGet(req *api.IndexGetRequest) (*api.IndexGetResponse, error) {
	id := req.GetIndexUuid()
	if index, ok := b.indexes[id]; !ok {
		return nil, util.ErrIndexNotFound
	} else {
		return &api.IndexGetResponse{Index: &sfd_db.IndexInfo{
			Uuid:      id,
			Size:      index.size(),
			LastSeqId: index.seqID,
		}}, nil
	}
}

func (b *backend) featureBatchAdd(req *sfd_db.FeatureBatchAddRequest) (*sfd_db.FeatureBatchAddResponse, error) {
	if !b.isMaster() {
		return nil, util.ErrNotLeader
	}

	indexID := req.GetColId()
	items := req.GetItems()
	index, ok := b.indexes[indexID]
	if !ok {
		return nil, util.ErrIndexNotFound
	}

	features := make([]*memoryFeature, 0, len(items))
	results := make([]*commonapis.Result, 0, len(items))
	ids := make([]string, 0, len(items))
	// 1. parse feature
	for i, v := range items {
		raw := parseFeature(v.GetFeature().GetBlob())
		if raw == nil {
			results = append(results, &commonapis.Result{Code: 3, Error: "invalid feature", Status: 0})
			ids = append(ids, "")
			continue
		}
		results = append(results, &commonapis.Result{Code: 0, Error: "", Status: 0})
		ids = append(ids, genFeatureID(indexID, b.gen.GetSeq()+int64(1+i)))
		features = append(features, &memoryFeature{
			raw: raw,
			key: v.GetKey(),
			seq: b.gen.GetSeq() + int64(1+i),
		})
	}

	// 2. TODO yore: write oplog
	if err := b.writePreflight(); err != nil {
		return nil, err
	}

	// 3. save to memory
	index.add(features)

	return &sfd_db.FeatureBatchAddResponse{
		Results: results,
		Ids:     ids,
	}, nil
}

func (b *backend) featureBatchDel(req *sfd_db.FeatureBatchDeleteRequest) (*sfd_db.FeatureBatchDeleteResponse, error) {
	if !b.isMaster() {
		return nil, util.ErrNotLeader
	}

	indexID := req.GetColId()
	ids := req.GetIds()

	index, ok := b.indexes[indexID]
	if !ok {
		return nil, util.ErrIndexNotFound
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
	if err := b.writePreflight(); err != nil {
		return nil, err
	}

	// TODO yore: change results when error

	// 3. delete in memory
	index.del(seqs)

	return &sfd_db.FeatureBatchDeleteResponse{Results: results}, nil
}

func (b *backend) featureSearch(req *sfd_db.FeatureBatchSearchRequest) (*sfd_db.FeatureBatchSearchResponse, error) {
	indexID := req.GetColId()
	items := req.GetFeatures()

	index, ok := b.indexes[indexID]
	if !ok {
		return nil, util.ErrIndexNotFound
	}

	results := make([]*commonapis.Result, len(items))
	resp := make([]*sfd_db.FeatureBatchSearchResponse_OneFeatureResult, len(items))
	features := make([][]float32, 0, len(items))
	mapping := make(map[int]int, len(items)) // features idx -> results idx
	for i, v := range items {
		f := parseFeature(v.GetBlob())
		if f == nil {
			results[i] = &commonapis.Result{Code: 3, Error: "invalid feature"}
			resp[i] = &sfd_db.FeatureBatchSearchResponse_OneFeatureResult{}
			continue
		}
		results[i] = &commonapis.Result{}
		mapping[len(features)] = i
		features = append(features, f)
	}

	for i, v := range features {
		res := index.search(v, int(req.GetTopK()))
		ret := make([]*sfd_db.FeatureBatchSearchResponse_SimilarResult, 0, len(res))
		for _, v := range res {
			ret = append(ret, &sfd_db.FeatureBatchSearchResponse_SimilarResult{
				Item:    &sfd_db.FeatureItem{Id: genFeatureID(indexID, v.seq)},
				Score:   float32(v.score),
				IndexId: indexID,
			})
		}
		resp[mapping[i]] = &sfd_db.FeatureBatchSearchResponse_OneFeatureResult{Results: ret}
	}

	return &sfd_db.FeatureBatchSearchResponse{
		Results:        results,
		FeatureResults: resp,
		ColId:          indexID,
		IsRefined:      true,
	}, nil
}

func (b *backend) SyncOplos() {
	for true {
		op, err := b.MongoClient.GetOplog(b.gen.GetSeq())
		if op != nil {
			if op.Operation == "add" {
				req := &sfd_db.FeatureBatchAddRequest{
					ColId: op.ColId,
				}
				blob, _ := base64.StdEncoding.DecodeString(op.Feature)
				feature := &commonapis.ObjectFeature{
					Type:    "",
					Version: 0,
					Blob:    blob,
				}
				featureItem := &sfd_db.FeatureItem{
					Id:        "",
					SeqId:     op.SeqId,
					Feature:   feature,
					ImageId:   "",
					ExtraInfo: "",
					MetaData:  nil,
					Key:       "",
				}
				req.Items = append(req.Items, featureItem)

				_, err := b.featureBatchAdd(req)
				if err != nil {
					b.logger.Errorf("SyncOplos featureBatchAdd error %v", err)
					continue
				}
				b.gen.NextSeq()
			} else if op.Operation == "del" {
				req := &sfd_db.FeatureBatchDeleteRequest{
					ColId: op.ColId,
				}
				req.Ids = append(req.Ids, op.FeatureId)
				_, err := b.featureBatchDel(req)
				if err != nil {
					b.logger.Errorf("SyncOplos featureBatchDel error %v", err)
					continue
				}
			}
		} else if err != nil {
			if err == mongo.ErrNoDocuments {
				return
			}
			b.logger.Errorf("syncOplogs err %v", err)
			return
		}
	}
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
