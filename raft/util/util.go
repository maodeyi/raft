package util

import (
	"errors"
	"github.com/maodeyi/raft/raft/proxy"
	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
	"strings"
)

func CheckLegalMaster(clusterInfo []*api.NodeInfo) (bool, []string) {
	var nodes []string
	number := len(clusterInfo)
	var healthNumber int32
	for _, v := range clusterInfo {
		if v.LastStatus {
			healthNumber++
		} else {
			nodes = append(nodes, v.Id)
		}
	}

	if healthNumber > int32(number/2+1) {
		return true, nodes
	}
	return false, nodes
}

func CloneClusterInfo(tags map[string]*proxy.Node) map[string]*proxy.Node {
	cloneTags := make(map[string]*proxy.Node)
	for k, v := range tags {
		cloneTags[k] = v
	}
	return cloneTags
}

func BuildNodeInfo(id, addrs string) (*api.NodeInfo, error) {
	addr := strings.Split(addrs, ":")
	if len(addr) < 2 {
		return nil, errors.New("error addrs format")
	}
	return &api.NodeInfo{
		Id:         id,
		Ip:         addr[0],
		Port:       addr[1],
		LastStatus: false,
		Role:       api.Role_SLAVE,
	}, nil
}
