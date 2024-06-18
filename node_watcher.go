package main

import (
	"context"
	"sync"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

type NodeWatcherEvent int

const (
	AddNode = iota
	DelNode
)

type NodeWatcherFn = func(nodeId uint64, url string)

type NodePeer struct {
	NodeID uint64
	Url    string
}

type NodeWatcherConf struct {
	Ctx    context.Context
	Client *clientv3.Client
	Prefix string
}

type NodeWatcher struct {
	events map[NodeWatcherEvent][]NodeWatcherFn
	ctx    context.Context
	cli    *clientv3.Client
	prefix string

	watching bool
	mu       sync.Mutex
}

func NewNodeWatcher(conf *NodeWatcherConf) *NodeWatcher {
	watcher := &NodeWatcher{
		ctx:    conf.Ctx,
		cli:    conf.Client,
		prefix: conf.Prefix,
		events: make(map[NodeWatcherEvent][]NodeWatcherFn),
	}
	watcher.events[AddNode] = make([]NodeWatcherFn, 0)
	watcher.events[DelNode] = make([]NodeWatcherFn, 0)

	return watcher
}

func (watcher *NodeWatcher) Subscribe(event NodeWatcherEvent, fn NodeWatcherFn) {
	watcher.events[event] = append(watcher.events[event], fn)
}

func (watcher *NodeWatcher) Watch() {
	if watcher.watching || !watcher.mu.TryLock() {
		return
	}
	defer watcher.mu.Unlock()
	watcher.watching = true
	go func() {
		defer func() {
			watcher.watching = false
		}()
		watchC := watcher.cli.Watch(watcher.ctx, watcher.prefix, clientv3.WithPrefix())
		for wresp := range watchC {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					watcher.handle(AddNode, string(ev.Kv.Key), string(ev.Kv.Value))
				case mvccpb.DELETE:
					watcher.handle(DelNode, string(ev.Kv.Key), string(ev.Kv.Value))
				}
			}
		}
	}()
}

func (watcher *NodeWatcher) GetNodes(ctx context.Context) ([]NodePeer, error) {
	resp, err := watcher.cli.Get(ctx, NodeExplorePath, clientv3.WithPrefix())
	if err != nil {
		return []NodePeer{}, err
	}

	var res []NodePeer

	for _, ev := range resp.Kvs {
		res = append(res, NodePeer{
			NodeID: ParseNodeId(string(ev.Key)),
			Url:    string(ev.Value),
		})
	}
	return res, nil
}

func (watcher *NodeWatcher) Close() {
	watcher.cli.Close()
}

func (watcher *NodeWatcher) handle(event NodeWatcherEvent, key string, val string) {
	nodeId := ParseNodeId(key)

	for _, fn := range watcher.events[event] {
		_fn := fn
		go func() {
			_fn(uint64(nodeId), val)
		}()
	}
}
