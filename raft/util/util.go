package util

import (
	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
)

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
