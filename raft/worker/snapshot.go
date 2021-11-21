package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// snapshot
// metadata:
type metadataSnapshot struct {
	Indexes map[string]int64 `json:"indexes"` // indexID -> seq
}

// index:
type indexSnapshot struct {
	IndexID  string                     `json:"index_id"`
	Seq      int64                      `json:"seq"`
	Features map[int64]*featureSnapshot `json:"features"`
}

// feature:
type featureSnapshot struct {
	Seq int64     `json:"seq"`
	Key string    `json:"key"`
	Raw []float32 `json:"raw"`
}

// snapshot all indexes
func (b *backend) snapshot() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m := make(map[string]int64, len(b.indexes))
	for indexID, index := range b.indexes {
		if seq, err := index.snapshot(); err != nil {
			b.logger.Errorf("snapshot index %v failed: %v", indexID, err)
			return
		} else {
			m[indexID] = seq
		}
	}
	meta := &metadataSnapshot{Indexes: m}
	data, _ := json.Marshal(meta)
	if err := ioutil.WriteFile(getMetadataSnapshotFileName(), data, 0755); err != nil {
		b.logger.Errorf("snapshot metadata failed: %v", err)
	}
}

func (m *memoryIndex) snapshot() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fs := make(map[int64]*featureSnapshot, m.size())
	for _, v := range m.features {
		fs[v.seq] = &featureSnapshot{
			Seq: v.seq,
			Key: v.key,
			Raw: v.raw,
		}
	}
	snap := &indexSnapshot{
		IndexID:  m.id,
		Seq:      m.seqID,
		Features: fs,
	}

	data, _ := json.Marshal(snap)
	return m.seqID, ioutil.WriteFile(getIndexSnapshotFileName(m.id, m.seqID), data, 0744)
}

func (m *memoryIndex) loadSnapshot(indexID string, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := ioutil.ReadFile(getIndexSnapshotFileName(indexID, seq))
	if err != nil {
		return err
	}
	snap := &indexSnapshot{}
	err = json.Unmarshal(data, snap)
	if err != nil {
		return err
	}
	fs := make(map[int64]*memoryFeature, len(snap.Features))
	for _, v := range snap.Features {
		fs[v.Seq] = &memoryFeature{
			raw: v.Raw,
			key: v.Key,
			seq: v.Seq,
		}
	}
	m.id = indexID
	m.seqID = seq
	m.features = fs
	return nil
}

// load all in recover
func (b *backend) loadSnapshot() {
	data, err := ioutil.ReadFile(getMetadataSnapshotFileName())
	if err != nil {
		b.logger.Errorf("read snapshot metadata failed: %v", err)
		return
	}

	snap := &metadataSnapshot{}
	if err := json.Unmarshal(data, snap); err != nil {
		b.logger.Errorf("invalid snapshot metadata file: %v", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.indexes == nil {
		b.indexes = make(map[string]*memoryIndex, len(snap.Indexes))
	}
	for indexID, seq := range snap.Indexes {
		index := &memoryIndex{}
		if err := index.loadSnapshot(indexID, seq); err != nil {
			b.logger.Errorf("load index %v:%v snapshot failed: %v", indexID, seq, err)
			continue
		}
		b.indexes[indexID] = index
		b.gen.seq = seq
	}
}

func getIndexSnapshotFileName(indexID string, seq int64) string {
	return fmt.Sprintf("index-%v-%v.json", indexID, seq)
}

func getMetadataSnapshotFileName() string {
	return "metadata.json"
}
