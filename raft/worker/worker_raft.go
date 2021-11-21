package worker

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/maodeyi/raft/raft/sniffer"
	"github.com/maodeyi/raft/raft/util"
	"github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	api "gitlab.bj.sensetime.com/mercury/protohub/api/engine-static-feature-db/index_rpc"
)

//todo change cluster node info

type Raft interface {
	GetRole() api.Role
	IsMaster() bool
	Healthy() bool
	LeaderElect()
	RoleNotify() <-chan api.Role
	SyncDoneNotify() <-chan bool
	NotifyStart() <-chan bool
	GetClusterInfo() []*api.NodeInfo
}

type HeartBead struct {
	Term     int32
	LeaderId int32
}

type HeartBeadReply struct {
	Term    int32
	Success bool
}

//TODO SYNC IS SYNC

type WorkerRaft struct {
	mu          sync.Mutex                                        // Lock to protect shared access to this peer's state
	peers       map[string]api.StaticFeatureDBWorkerServiceClient // RPC end points of all peers
	clusterInfo []*api.NodeInfo
	sniffer     *sniffer.Sniffer
	backend     Worker
	logger      *logrus.Entry
	me          string // this peer's index into peers[]
	syncdone    chan bool
	closeCh     chan struct{}

	workerNum   int32
	currentTerm int32
	votedFor    string

	seqId int32

	state           api.Role
	electionTimeout int
	grantVoteCh     chan bool
	heartBeatCh     chan bool
	isMasterCh      chan api.Role
	leaderCh        chan bool
	startCh         chan bool
	totalVotes      int
	timer           *time.Timer
}

func (rf *WorkerRaft) addPeer(nodeInfo *api.NodeInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, nodeInfo.Ip+":"+nodeInfo.Port, grpc.WithInsecure())

	if err != nil {
		rf.clusterInfo = append(rf.clusterInfo, nodeInfo)
		rf.peers[nodeInfo.Id] = nil
		return err
	}

	nodeInfo.LastStatus = false
	rf.clusterInfo = append(rf.clusterInfo, nodeInfo)

	rf.peers[nodeInfo.Id] = api.NewStaticFeatureDBWorkerServiceClient(conn)
	return nil
}

func (rf *WorkerRaft) subWokersStauts() {
	//todo every node has same name???????
	addrs, ch := rf.sniffer.Subscribe("worker")
	for k, v := range addrs {
		rf.mu.Lock()
		if len(rf.peers) > int(rf.workerNum/2) {
			rf.startCh <- true
		}

		if len(rf.peers) == int(rf.workerNum) {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		nodeInfo, err := util.BuildNodeInfo(k, v)
		if err != nil {
			rf.logger.Errorf("BuildNodeInfo error %s %s %v", k, v, err)
			continue
		}
		rf.mu.Lock()
		err = rf.addPeer(nodeInfo)
		rf.mu.Unlock()
		if err != nil {
			rf.logger.Errorf("proxy inti worker peer error %v", err)
			close(rf.closeCh)
			return
		}

	}

	for true {
		select {
		case <-rf.closeCh:
			return
		case msg := <-ch:
			if msg.Begin {
				rf.mu.Lock()
				_, ok := rf.peers[msg.WorkerID]
				if !ok && len(rf.peers) < int(rf.workerNum) {
					nodeInfo, err := util.BuildNodeInfo(msg.WorkerID, msg.Address)
					if err != nil {
						rf.logger.Errorf("BuildNodeInfo error %s %s %v", msg.WorkerID, msg.Address, err)
					}
					err = rf.addPeer(nodeInfo)
					if err != nil {
						rf.logger.Errorf("worker inti node peer error %v", err)
					}
					if len(rf.peers) == int(rf.workerNum/2+1) {
						select {
						case rf.startCh <- true:
						default:
						}
					}
				}
				rf.mu.Unlock()
			}
		}
	}
}

//todo get workernumber from mongo
func (rf *WorkerRaft) Init(backend *backend) error {
	rf.peers = make(map[string]api.StaticFeatureDBWorkerServiceClient)
	go rf.subWokersStauts()

	//todo get self id ip
	rf.me = me
	rf.backend = backend
	rf.currentTerm = 0
	rf.votedFor = ""
	rf.seqId = 0
	rf.logger = logrus.StandardLogger().WithField("component", "worker_raft")

	rf.state = api.Role_SLAVE
	rf.syncdone = make(chan bool)
	rf.closeCh = make(chan struct{})
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.isMasterCh = make(chan api.Role)
	rf.leaderCh = make(chan bool)
	rf.totalVotes = 0
	rf.logger.Infof("--------------------- server %d  init state ---------------------\n", rf.me)
	return nil
}

func (rf *WorkerRaft) Close() {
	close(rf.closeCh)
	rf.sniffer.Stop()
}

func (rf *WorkerRaft) LeaderElect() {
	rf.backend.subscribeOpLogs()
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	go func() {
		for {
			select {
			case <-rf.closeCh:
				rf.logger.Infof("close worker raft")
				return
			default:
				time.Sleep(200 * time.Millisecond)
				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()
				switch {
				case state == api.Role_MASTER:
					rf.logger.Infof("Candidate %d: l become leader now!!! Current term is %d\n", rf.me, rf.currentTerm)
					rf.startHeartBeat()
				case state == api.Role_Candidate:
					rf.logger.Infof("================ Candidate %d start election!!! ================\n", rf.me)
					go rf.startRequestVote()
					select {
					case <-rf.heartBeatCh:
						rf.logger.Infof("Candidate %d: receive heartbeat when requesting votes, turn back to follower\n", rf.me)
						rf.mu.Lock()
						rf.convertToFollower(rf.currentTerm, "")
						rf.mu.Unlock()
					case <-rf.leaderCh:
					case <-rf.timer.C:
						rf.mu.Lock()
						if rf.state == api.Role_SLAVE {
							rf.logger.Infof("Candidate %d: existing a higher term candidate, withdraw from the election\n", rf.me)
							rf.mu.Unlock()
							continue
						}
						rf.convertToCandidate()
						rf.mu.Unlock()
					}
				case state == api.Role_SLAVE:
					rf.mu.Lock()
					rf.drainOldTimer()
					rf.electionTimeout = GenerateElectionTimeout(200, 400)
					rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
					rf.mu.Unlock()
					select {
					case <-rf.grantVoteCh:
						rf.logger.Infof("Server %d: reset election time due to grantVote\n", rf.me)
					case <-rf.heartBeatCh:
						rf.logger.Infof("Server %d: reset election time due to heartbeat\n", rf.me)
					case <-rf.timer.C:
						rf.logger.Infof("Server %d: election timeout, turn to candidate\n", rf.me)
						rf.mu.Lock()
						rf.convertToCandidate()
						rf.mu.Unlock()
					}
				}
			}
		}
	}()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *WorkerRaft) GetState() (int32, bool) {
	var term int32
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == api.Role_MASTER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *WorkerRaft) persist() {
	//w := new(bytes.Buffer)
	//e := gob.NewEncoder(w)
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedFor)
	////e.Encode(rf.log)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
}

func (rf *WorkerRaft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	//d.Decode(&rf.log)
	//rf.seq_id = int32(len(rf.log))
	if data == nil || len(data) < 1 {
		return
	}
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	SeqId       int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *WorkerRaft) RequestVoteChannel(args *api.RequestVoteRequest) *api.RequestVoteResponse {
	reply := api.RequestVoteResponse{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Infof("Server %d: got RequestVote from candidate %d, args: %+v, current currentTerm: %d, current seq_id: %v\n", rf.me, args.CandidateId, args, rf.currentTerm, rf.seqId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term == rf.currentTerm {
			if rf.votedFor != "" && rf.votedFor != args.CandidateId {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				//todo   any time, we will get a server with max seq_id
				if args.SeqId <= rf.seqId {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
				} else {
					rf.logger.Infof("Server %d: grant vote to candidate %d  args.seqid %d rf.seq_id %d \n", rf.me, args.CandidateId, args.SeqId, rf.seqId)
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.persist()
					rf.setGrantVoteCh()
				}
			}
		} else {
			rf.convertToFollower(args.Term, "")
			rf.logger.Infof("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.setGrantVoteCh()
		}
	}
	rf.logger.Infof("======= server %d got RequestVote from candidate %d, args: %+v, current seq_id: %v, reply: %+v =======\n", rf.me, args.CandidateId, args, rf.seqId, reply)
	return &reply
}

//func (rf *RaftHTTPService) Start(command interface{}) (int32, int32, bool) {
//	rf.mu.Lock()
//	term := rf.currentTerm
//	isLeader := (rf.state == api.Role_MASTER)
//
//	if isLeader {
//		DPrintf("Leader %d: got a new Start task, command: %v\n", rf.me, command)
//		if rf.syncdone == false {
//			rf.syncOpLogs()
//			rf.syncdone = true
//			DPrintf("leader is syncing")
//		}
//		rf.seq_id++
//		//rf.log = append(rf.log, Entry{rf.currentTerm, command})
//		err := rf.mongoclient.InsertOpLog()
//		if err != nil {
//			DPrintf("mongo InsertOpLog error %v", err)
//		}
//		rf.persist()
//	}
//	rf.mu.Unlock()
//	//err := rf.mongoclient.InsertOpLog()
//	//if err != nil {
//	//	DPrintf("mongo InsertOpLog error %v", err)
//	//}
//	return rf.seq_id, term, isLeader
//}

func (rf *WorkerRaft) Kill() {
	//atomic.StoreInt32(&rf.dead, 1)
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max-min) + min
	return randNum
}

//func (rf *WorkerRaft) subscribeOpLogs() {
//	go func() {
//		for true {
//			select {
//			//when raft change into follower from master receive from subscribeOplogsCh
//			case isMaster := <-rf.isMasterCh:
//				if isMaster == api.Role_MASTER {
//					rf.logger.Infof("turn to master stop subcribe and sync log")
//					break
//				}
//			default:
//			}
//			time.Sleep(300 * time.Millisecond)
//			addseq_id, _ := rf.GetOpLogs()
//			if addseq_id == 1 {
//				rf.mu.Lock()
//				rf.seq_id++
//				rf.mu.Unlock()
//			}
//		}
//	}()
//}

func (rf *WorkerRaft) startHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != api.Role_MASTER {
			rf.mu.Unlock()
			return
		}
		rf.logger.Infof("Leader %d: start sending heartbeat, current term: %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		for index, v := range rf.clusterInfo {
			go func(ii string, index int) {
				if ii == rf.me {
					return
				}
				for {
					time.Sleep(200 * time.Millisecond)
					rf.mu.Lock()
					if rf.state != api.Role_MASTER {
						rf.mu.Unlock()
						return
					}

					args := api.HeartBeadRequest{
						Term:   rf.currentTerm,
						LeadId: rf.me,
					}
					rf.mu.Unlock()
					reply, ok := rf.SendHeartBeatReply(rf.peers[ii], &args)
					if ok {
						rf.mu.Lock()
						rf.clusterInfo[index].LastStatus = true
						if reply.Term > rf.currentTerm {
							rf.logger.Infof("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, ii)
							rf.convertToFollower(reply.Term, "")
							rf.mu.Unlock()
							return
						}
						if rf.currentTerm != args.Term || rf.state != api.Role_MASTER {
							rf.mu.Unlock()
							return
						}

						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.clusterInfo[index].LastStatus = false
						rf.mu.Unlock()
						rf.logger.Infof("Leader %d: sending HeartBeat to server %d failed\n", rf.me, ii)
						return
					}
				}
			}(v.Id, index)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func (rf *WorkerRaft) startRequestVote() {
	rf.mu.Lock()
	if rf.state != api.Role_Candidate {
		rf.logger.Infof("no candiate")
		rf.mu.Unlock()
		return
	}

	args := api.RequestVoteRequest{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		SeqId:       rf.seqId,
	}

	nLeader := 0
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		index := index
		go func(ii string) {
			if ii == rf.me {
				return
			}
			reply, ok := rf.sendRequestVote(rf.peers[index], &args)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term, "")
					rf.mu.Unlock()
					return
				}

				if rf.currentTerm != args.Term || rf.state != api.Role_Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					rf.totalVotes++

					if nLeader == 0 && rf.totalVotes > len(rf.peers)/2 && rf.state == api.Role_Candidate {
						nLeader++
						rf.isMasterCh <- api.Role_MASTER
						rf.convertToLeader()
						rf.setLeaderCh()
						go func() {
							rf.backend.SyncOplogs()
						}()
					}
				}
				rf.mu.Unlock()
			} else {
				rf.logger.Infof("Candidate %d: sending RequestVote to server %d failed\n", rf.me, ii)
			}
		}(index)
	}
}

func (rf *WorkerRaft) sendRequestVote(client api.StaticFeatureDBWorkerServiceClient, request *api.RequestVoteRequest) (*api.RequestVoteResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if client == nil {
		return nil, false
	}

	response, err := client.RequestVote(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

func (rf *WorkerRaft) convertToFollower(term int32, voteFor string) {
	rf.currentTerm = term
	rf.state = api.Role_SLAVE
	rf.totalVotes = 0
	rf.votedFor = voteFor
	if rf.state == api.Role_MASTER {
		rf.syncdone <- false
		rf.backend.subscribeOpLogs()
		rf.isMasterCh <- api.Role_SLAVE
	}
	rf.persist()
}

func (rf *WorkerRaft) HeartBeatChannel(args *api.HeartBeadRequest) *api.HeartBeadResponse {
	rf.mu.Lock()
	reply := api.HeartBeadResponse{}
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.clusterInfo = args.ClusterInfo.NodeInfo
		rf.setHeartBeatCh()
		rf.convertToFollower(args.Term, args.LeadId)
	}
	rf.logger.Infof("======= server %d got HeartBeart from leader %d, args: %+v, current term: %v, reply: %+v =======\n", rf.me, args.LeadId, args, rf.currentTerm, reply)
	return &reply
}

func (rf *WorkerRaft) SendHeartBeatReply(client api.StaticFeatureDBWorkerServiceClient, request *api.HeartBeadRequest) (*api.HeartBeadResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	response, err := client.HeartBead(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

func (rf *WorkerRaft) convertToCandidate() {
	rf.state = api.Role_Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *WorkerRaft) convertToLeader() {
	//may be election again
	rf.state = api.Role_MASTER
}

func (rf *WorkerRaft) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *WorkerRaft) setGrantVoteCh() {
	go func() {
		select {
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *WorkerRaft) setLeaderCh() {
	go func() {
		select {
		case <-rf.leaderCh:
		default:
		}
		rf.leaderCh <- true
	}()
}

func (rf *WorkerRaft) drainOldTimer() {
	select {
	case <-rf.timer.C:
		rf.logger.Infof("Server %d: drain the old timer\n", rf.me)
	default:
	}
}

type NodeStatus struct {
	role    api.Role
	healthy bool
}

func (rf *WorkerRaft) GetNodeStatus() *NodeStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	healthy, _ := util.CheckLegalMaster(rf.clusterInfo)
	nodeStatus := &NodeStatus{
		role:    rf.state,
		healthy: healthy,
	}
	return nodeStatus
}

func (rf *WorkerRaft) IsMaster() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != api.Role_MASTER {
		return false
	}
	ok, _ := util.CheckLegalMaster(rf.clusterInfo)
	return ok
}

func (rf *WorkerRaft) RoleNotify() <-chan api.Role {
	return rf.isMasterCh
}

func (rf *WorkerRaft) SyncDoneNotify() <-chan bool {
	return rf.syncdone
}

func (rf *WorkerRaft) NotifyStart() <-chan bool {
	return rf.startCh
}

func (rf *WorkerRaft) Healthy() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok, _ := util.CheckLegalMaster(rf.clusterInfo)
	return ok
}

func (rf *WorkerRaft) GetClusterInfo() []*api.NodeInfo {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.clusterInfo
}

func (rf *WorkerRaft) GetRole() api.Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}
