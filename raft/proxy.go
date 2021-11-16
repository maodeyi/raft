package raft

type Proxy struct{}

func NewProxy() *Proxy {
	rf := &Proxy{}
	return rf
}

func (s *Proxy) Init() error {
	return nil
}
