package worker

import "sync"

type generator struct {
	mu  sync.Mutex
	seq int64
}

func newGenerator(lastSeq int64) *generator {
	return &generator{
		mu:  sync.Mutex{},
		seq: lastSeq,
	}
}

//todo get and add when insert must be automitic

func (g *generator) GetSeq() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seq
}

func (g *generator) NextSeq() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seq++
	return g.seq
}

// Rollback 之前的 save oplog 不会失败, 所以调用 NextSeq 没有问题,
// 但现在与 MongoDB 通信, 有可能失败, 此时就要回滚 SeqID.
func (g *generator) Rollback() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seq--
}
