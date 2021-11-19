package raft

import (
	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func CheckLegalMaster(clusterInfo []*raft_api.NodeInfo) bool {
	number := len(clusterInfo)
	var healthNumber int32
	for _, v := range clusterInfo {
		if v.LastStatus {
			healthNumber++
		}
	}

	if healthNumber > int32(number/2+1) {
		return true
	}
	return false
}
