package main

import (
	"context"

	"go.etcd.io/etcd/clientv3"
)

type NodeListenLeaseFn = func(*clientv3.LeaseKeepAliveResponse)

type NodeRegisterConf struct {
	Ctx    context.Context
	Client *clientv3.Client //etcd client
	NodeID uint64
	Url    string
}

type NodeRegister struct {
	ctx           context.Context
	cli           *clientv3.Client //etcd client
	leaseID       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	nodeID        uint64
	url           string

	subscribers []NodeListenLeaseFn
}

func NewNodeRegister(conf *NodeRegisterConf) *NodeRegister {
	ser := &NodeRegister{
		ctx:    conf.Ctx,
		cli:    conf.Client,
		nodeID: conf.NodeID,
		url:    conf.Url,
	}

	return ser
}

func (s *NodeRegister) Apply(leaseTTL int64) error {
	// use snow flake
	if err := s.putKeyWithLease(leaseTTL); err != nil {
		return err
	}

	go func() {
		for leaseKeepResp := range s.keepAliveChan {
			s.handle(leaseKeepResp)
		}
	}()

	return nil
}

func (s *NodeRegister) putKeyWithLease(lease int64) error {
	resp, err := s.cli.Grant(s.ctx, lease)
	if err != nil {
		return err
	}
	_, err = s.cli.Put(s.ctx, GetPath(s.nodeID), s.url, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}

	leaseRespChan, err := s.cli.KeepAlive(s.ctx, resp.ID)

	if err != nil {
		return err
	}
	s.leaseID = resp.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

func (s *NodeRegister) SubscribeListenLease(fn NodeListenLeaseFn) {
	s.subscribers = append(s.subscribers, fn)
}

func (s *NodeRegister) Close() error {
	if _, err := s.cli.Revoke(context.Background(), s.leaseID); err != nil {
		return err
	}
	return s.cli.Close()
}

func (s *NodeRegister) handle(resp *clientv3.LeaseKeepAliveResponse) {
	for _, fn := range s.subscribers {
		go func() {
			fn(resp)
		}()
	}
}
