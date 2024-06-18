package main

import (
	"fmt"
	"strconv"
	"strings"
)

func GetPath(nodeID uint64) string {
	return fmt.Sprintf("%s%d", NodeExplorePath, nodeID)
}

func ParseNodeId(path string) uint64 {
	str := strings.Split(path, "/")[2]
	nodeID, _ := strconv.Atoi(str)
	return uint64(nodeID)
}
