package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"

	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
)

//todo change cluster node info

type HeartBead struct {
	Term     int32
	LeaderId int32
}

type HeartBeadReply struct {
	Term    int32
	Success bool
}

type NodeInfo struct {
	Id         string
	Ip         string
	Port       string
	LastStatus bool
	Role       raft_api.Role
}

type Worker_Raft struct {
	mu          sync.Mutex                   // Lock to protect shared access to this peer's state
	peers       []raft_api.RaftServiceClient // RPC end points of all peers
	clusterInfo map[int]NodeInfo
	persister   *Persister // Object to hold this peer's persisted state
	mongoclient *MongoClient
	me          int32 // this peer's index into peers[]
	syncdone    bool

	currentTerm int32
	votedFor    int32

	seq_id int32

	state             raft_api.Role
	electionTimeout   int
	grantVoteCh       chan bool
	heartBeatCh       chan bool
	subscribeOplogsCh chan bool
	leaderCh          chan bool
	totalVotes        int
	timer             *time.Timer
}

//todo add worker_id
func (rf *Worker_Raft) initPeer() (client raft_api.RaftServiceClient, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "127.0.0.1:8080", grpc.WithInsecure())

	rf.mu.Lock()
	defer rf.mu.Unlock()
	nodeInfo := NodeInfo{
		Id:   "1",
		Ip:   "127.0.0.1",
		Port: "8080",
		Role: raft_api.Role_SLAVE,
	}
	if err != nil {
		nodeInfo.LastStatus = false
		rf.clusterInfo[0] = nodeInfo
		return nil, err
	}
	nodeInfo.LastStatus = true
	rf.clusterInfo[0] = nodeInfo
	return raft_api.NewRaftServiceClient(conn), nil
}

func (rf *Worker_Raft) Init() error {
	rf.clusterInfo = make(map[int]NodeInfo)
	//todo init peers
	for i := 0; i < 5; i++ {
		peerClient, err := rf.initPeer()
		if err != nil {
			return err
		}
		rf.peers = append(rf.peers, peerClient)
	}

	//todo persister
	//rf.persister = persister
	//rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.seq_id = 0 //
	//rf.log = []Entry{}
	var err error
	rf.mongoclient, err = GetMongoClient()
	if err != nil {
		DPrintf("get mongo client error %v", err)
		panic(err)
	}

	rf.state = raft_api.Role_SLAVE
	rf.syncdone = false

	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.subscribeOplogsCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.totalVotes = 0
	// initialize from state persisted before a crash
	//todo readPersist
	//rf.readPersist(persister.ReadRaftState())
	DPrintf("--------------------- server %d  init state ---------------------\n", rf.me)
	return nil
}

func (rf *Worker_Raft) Run() {
	rf.subscribeOpLogs()
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch {
			case state == raft_api.Role_MASTER:
				DPrintf("Candidate %d: l become leader now!!! Current term is %d\n", rf.me, rf.currentTerm)
				rf.startHeartBeat()
			case state == raft_api.Role_Candidate:
				DPrintf("================ Candidate %d start election!!! ================\n", rf.me)
				go rf.startRequestVote()
				select {
				case <-rf.heartBeatCh:
					DPrintf("Candidate %d: receive heartbeat when requesting votes, turn back to follower\n", rf.me)
					rf.mu.Lock()
					rf.convertToFollower(rf.currentTerm, -1)
					rf.mu.Unlock()
				case <-rf.leaderCh:
				case <-rf.timer.C:
					rf.mu.Lock()
					if rf.state == raft_api.Role_SLAVE {
						DPrintf("Candidate %d: existing a higher term candidate, withdraw from the election\n", rf.me)
						rf.mu.Unlock()
						continue
					}
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case state == raft_api.Role_SLAVE:
				rf.mu.Lock()
				rf.drainOldTimer()
				rf.electionTimeout = GenerateElectionTimeout(200, 400)
				rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				rf.mu.Unlock()
				select {
				case <-rf.grantVoteCh:
					DPrintf("Server %d: reset election time due to grantVote\n", rf.me)
				case <-rf.heartBeatCh:
					DPrintf("Server %d: reset election time due to heartbeat\n", rf.me)
				case <-rf.timer.C:
					DPrintf("Server %d: election timeout, turn to candidate\n", rf.me)
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			}
		}
	}()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Worker_Raft) GetState() (int32, bool) {
	var term int32
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == raft_api.Role_MASTER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Worker_Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Worker_Raft) readPersist(data []byte) {
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
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	SeqId       int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Worker_Raft) RequestVoteChannel(args *raft_api.RequestVoteRequest) *raft_api.RequestVoteResponse {
	reply := raft_api.RequestVoteResponse{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got RequestVote from candidate %d, args: %+v, current currentTerm: %d, current seq_id: %v\n", rf.me, args.CandidateId, args, rf.currentTerm, rf.seq_id)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term == rf.currentTerm {
			if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				//todo   any time, we will get a server with max seq_id
				if args.SeqId <= rf.seq_id {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
				} else {
					DPrintf("Server %d: grant vote to candidate %d  args.seqid %d rf.seq_id %d \n", rf.me, args.CandidateId, args.SeqId, rf.seq_id)
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.persist()
					rf.setGrantVoteCh()
				}
			}
		} else {
			rf.convertToFollower(args.Term, -1)
			DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.setGrantVoteCh()
		}
	}
	DPrintf("======= server %d got RequestVote from candidate %d, args: %+v, current seq_id: %v, reply: %+v =======\n", rf.me, args.CandidateId, args, rf.seq_id, reply)
	return &reply
}

//func (rf *RaftHTTPService) Start(command interface{}) (int32, int32, bool) {
//	rf.mu.Lock()
//	term := rf.currentTerm
//	isLeader := (rf.state == raft_api.Role_MASTER)
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

func (rf *Worker_Raft) Kill() {
	//atomic.StoreInt32(&rf.dead, 1)
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max-min) + min
	return randNum
}

func (rf *Worker_Raft) syncOpLogs() {
	for true {
		addseq_id, err := rf.GetOpLogs()
		if addseq_id == 1 {
			rf.seq_id++
		} else if addseq_id == 0 && err == nil {
			return
		} else if err != nil {
			DPrintf("syncOplogs err %v", err)
			return
		}
	}
}

func (rf *Worker_Raft) subscribeOpLogs() {
	go func() {
		for true {
			select {
			//when raft change into follower from master receive from subscribeOplogsCh
			case <-rf.subscribeOplogsCh:
				DPrintf("turn to master stop subcribe and sync log")
				break
			default:
			}
			time.Sleep(300 * time.Millisecond)
			addseq_id, _ := rf.GetOpLogs()
			if addseq_id == 1 {
				rf.mu.Lock()
				rf.seq_id++
				rf.mu.Unlock()
			}
		}
	}()
}

func (rf *Worker_Raft) startHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != raft_api.Role_MASTER {
			rf.mu.Unlock()
			return
		}
		DPrintf("Leader %d: start sending heartbeat, current term: %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			go func(ii int32) {
				if ii == rf.me {
					return
				}
				for {
					time.Sleep(200 * time.Millisecond)
					rf.mu.Lock()
					if rf.state != raft_api.Role_MASTER {
						rf.mu.Unlock()
						return
					}

					args := raft_api.HeartBeadRequest{
						Term:   rf.currentTerm,
						LeadId: rf.me,
					}
					rf.mu.Unlock()
					reply, ok := rf.SendHeartBeatReply(ii, &args)
					if ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							DPrintf("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, ii)
							rf.subscribeOpLogs()
							rf.syncdone = false
							rf.convertToFollower(reply.Term, -1)
							rf.mu.Unlock()
							return
						}
						if rf.currentTerm != args.Term || rf.state != raft_api.Role_MASTER {
							rf.mu.Unlock()
							return
						}

						rf.mu.Unlock()
					} else {
						DPrintf("Leader %d: sending HeartBeat to server %d failed\n", rf.me, ii)
						return
					}
				}
			}(int32(i))
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func (rf *Worker_Raft) startRequestVote() {
	rf.mu.Lock()
	if rf.state != raft_api.Role_Candidate {
		DPrintf("no candiate")
		rf.mu.Unlock()
		return
	}

	args := raft_api.RequestVoteRequest{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		SeqId:       rf.seq_id,
	}

	nLeader := 0
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		go func(ii int32) {
			if ii == rf.me {
				return
			}
			reply, ok := rf.sendRequestVote(ii, &args)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term, -1)
					rf.mu.Unlock()
					return
				}

				if rf.currentTerm != args.Term || rf.state != raft_api.Role_Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					rf.totalVotes++

					if nLeader == 0 && rf.totalVotes > len(rf.peers)/2 && rf.state == raft_api.Role_Candidate {
						nLeader++
						rf.subscribeOplogsCh <- false
						rf.convertToLeader()
						rf.setLeaderCh()
						go func() {
							rf.mu.Lock()
							rf.syncOpLogs()
							rf.syncdone = true
							rf.mu.Unlock()
						}()
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Candidate %d: sending RequestVote to server %d failed\n", rf.me, ii)
			}
		}(int32(i))
	}
}

func (rf *Worker_Raft) sendRequestVote(server int32, request *raft_api.RequestVoteRequest) (*raft_api.RequestVoteResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	response, err := rf.peers[server].RequestVote(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

func (rf *Worker_Raft) convertToFollower(term int32, voteFor int32) {
	rf.currentTerm = term
	rf.state = raft_api.Role_SLAVE
	rf.totalVotes = 0
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *Worker_Raft) HeartBeatChannel(args *raft_api.HeartBeadRequest) *raft_api.HeartBeadResponse {
	rf.mu.Lock()
	//todo change rf.peers
	reply := raft_api.HeartBeadResponse{}
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.setHeartBeatCh()
		rf.convertToFollower(args.Term, args.LeadId)
	}
	DPrintf("======= server %d got HeartBeart from leader %d, args: %+v, current term: %v, reply: %+v =======\n", rf.me, args.LeadId, args, rf.currentTerm, reply)
	return &reply
}

func (rf *Worker_Raft) SendHeartBeatReply(server int32, request *raft_api.HeartBeadRequest) (*raft_api.HeartBeadResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	response, err := rf.peers[server].HeartBead(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

func (rf *Worker_Raft) convertToCandidate() {
	rf.state = raft_api.Role_Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *Worker_Raft) GetOpLogs() (int, error) {
	_, err := rf.mongoclient.GetOplog()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return 1, nil
}

func (rf *Worker_Raft) convertToLeader() {
	//may be election again
	rf.state = raft_api.Role_MASTER
}

func (rf *Worker_Raft) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *Worker_Raft) setGrantVoteCh() {
	go func() {
		select {
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *Worker_Raft) setLeaderCh() {
	go func() {
		select {
		case <-rf.leaderCh:
		default:
		}
		rf.leaderCh <- true
	}()
}

func (rf *Worker_Raft) drainOldTimer() {
	select {
	case <-rf.timer.C:
		DPrintf("Server %d: drain the old timer\n", rf.me)
	default:
	}
}
