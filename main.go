package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	manager := NewNodeManager(&NodeManagerConf{
		Context:     context.Background(),
		Endpoints:   []string{"localhost:2479"},
		DialTimeout: 10 * time.Second,
	})

	watcher, err := manager.GetWatch()
	if err != nil {
		panic(err)
	}
	watcher.Watch()

	watcher.Subscribe(AddNode, func(nodeId uint64, url string) {
		fmt.Printf("Add node %d url %s %s\n", nodeId, url, time.Now().Local().Format(time.ANSIC))
	})
	watcher.Subscribe(DelNode, func(nodeId uint64, url string) {
		fmt.Printf("node %d url %s removed %s\n", nodeId, url, time.Now().Local().Format(time.ANSIC))
	})

	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)
		register, err := manager.GetRegister(uint64(i+1), fmt.Sprintf("localhost:800%d", i))
		if err != nil {
			panic(err)
		}
		if err := register.Apply(5); err != nil {
			panic(err)
		}

		go func() {
			<-time.After(5 * time.Second)
			register.Close()
		}()
	}

	<-time.After(5 * time.Second)

	fmt.Println("main done")
}
