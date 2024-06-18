package main

import (
	"go.etcd.io/etcd/clientv3"
)

type NodeManagerConf clientv3.Config

type NodeManager struct {
	conf *NodeManagerConf
}

func NewNodeManager(conf *NodeManagerConf) *NodeManager {

	return &NodeManager{
		conf: conf,
	}
}

func (manager *NodeManager) GetWatch() (*NodeWatcher, error) {
	conf := manager.conf
	cli, err := clientv3.New(clientv3.Config{
		Context:     conf.Context,
		Endpoints:   conf.Endpoints,
		DialTimeout: conf.DialTimeout,
	})
	if err != nil {
		return nil, err
	}

	return NewNodeWatcher(&NodeWatcherConf{
		Ctx:    conf.Context,
		Client: cli,
		Prefix: NodeExplorePath,
	}), nil
}

func (manager *NodeManager) GetRegister(nodeID uint64, url string) (*NodeRegister, error) {
	conf := manager.conf
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Endpoints,
		DialTimeout: conf.DialTimeout,
	})
	if err != nil {
		return nil, err
	}

	return NewNodeRegister(&NodeRegisterConf{
		Ctx:    conf.Context,
		Client: cli,
		NodeID: nodeID,
		Url:    url,
	}), nil
}
