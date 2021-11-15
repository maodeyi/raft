package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	//labrpc2 "github.com/maodeyi/raft/labrpc"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"time"

	raft_api "gitlab.bj.sensetime.com/mercury/protohub/api/raft"
)

//todo change cluster node info
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type HeartBead struct {
	Term     int32
	LeaderId int32
}

type HeartBeadReply struct {
	Term    int32
	Success bool
}

const (
	Follower  string = "follower"
	Candidate        = "candidate"
	Leader           = "leader"
)

type Entry struct {
	Term    int32
	Command interface{}
}

type RaftHTTPService struct {
	mu          sync.Mutex                   // Lock to protect shared access to this peer's state
	peers       []raft_api.RaftServiceClient // RPC end points of all peers
	persister   *Persister                   // Object to hold this peer's persisted state
	mongoclient *MongoClient
	me          int32 // this peer's index into peers[]
	syncdone    bool

	currentTerm int32
	votedFor    int32

	seq_id int32
	log    []Entry

	state             string
	electionTimeout   int
	applyCh           chan ApplyMsg
	grantVoteCh       chan bool
	heartBeatCh       chan bool
	subscribeOplogsCh chan bool
	leaderCh          chan bool
	totalVotes        int
	timer             *time.Timer
}

func NewRaftHTTPService() *RaftHTTPService {
	rf := &RaftHTTPService{}
	return rf
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *RaftHTTPService) initPeer() (client raft_api.RaftServiceClient, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return raft_api.NewRaftServiceClient(conn), nil
}

func (rf *RaftHTTPService) Init() error {
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
	rf.log = []Entry{}
	var err error
	rf.mongoclient, err = GetMongoClient()
	if err != nil {
		DPrintf("get mongo client error %v", err)
		panic(err)
	}

	rf.state = Follower
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

func (rf *RaftHTTPService) Run() {
	rf.subscribeOpLogs()
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch {
			case state == Leader:
				DPrintf("Candidate %d: l become leader now!!! Current term is %d\n", rf.me, rf.currentTerm)
				rf.startHeartBeat()
			case state == Candidate:
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
					if rf.state == Follower {
						DPrintf("Candidate %d: existing a higher term candidate, withdraw from the election\n", rf.me)
						rf.mu.Unlock()
						continue
					}
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case state == Follower:
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
func (rf *RaftHTTPService) GetState() (int32, bool) {

	var term int32
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *RaftHTTPService) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *RaftHTTPService) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.seq_id = int32(len(rf.log))
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

func (rf *RaftHTTPService) RequestVoteChannel(args *raft_api.RequestVoteRequest) *raft_api.RequestVoteResponse {
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

func (rf *RaftHTTPService) Start(command interface{}) (int32, int32, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		DPrintf("Leader %d: got a new Start task, command: %v\n", rf.me, command)
		if rf.syncdone == false {
			rf.syncOpLogs()
			rf.syncdone = true
			DPrintf("leader is syncing")
		}
		rf.seq_id++
		rf.log = append(rf.log, Entry{rf.currentTerm, command})
		err := rf.mongoclient.InsertOpLog()
		if err != nil {
			DPrintf("mongo InsertOpLog error %v", err)
		}
		rf.persist()
	}
	rf.mu.Unlock()
	//err := rf.mongoclient.InsertOpLog()
	//if err != nil {
	//	DPrintf("mongo InsertOpLog error %v", err)
	//}
	return rf.seq_id, term, isLeader
}

func (rf *RaftHTTPService) Kill() {
	//atomic.StoreInt32(&rf.dead, 1)
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max-min) + min
	return randNum
}

func (rf *RaftHTTPService) syncOpLogs() {
	for true {
		addseq_id, err := rf.GetOpLogs()
		if addseq_id == 1 {
			rf.mu.Lock()
			rf.seq_id++
			rf.mu.Unlock()
		} else if addseq_id == 0 && err == nil {
			return
		} else if err != nil {
			DPrintf("syncOplogs err %v", err)
			return
		}
	}
}

func (rf *RaftHTTPService) subscribeOpLogs() {
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

func (rf *RaftHTTPService) startHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
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
					if rf.state != Leader {
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
						if rf.currentTerm != args.Term || rf.state != Leader {
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

func (rf *RaftHTTPService) startRequestVote() {
	rf.mu.Lock()
	if rf.state != Candidate {
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

				if rf.currentTerm != args.Term || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					rf.totalVotes++

					if nLeader == 0 && rf.totalVotes > len(rf.peers)/2 && rf.state == Candidate {
						nLeader++
						//todo to do syncoplogs take long time ,so other follower election again????,
						rf.subscribeOplogsCh <- false
						rf.syncOpLogs()
						rf.convertToLeader()
						rf.setLeaderCh()
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Candidate %d: sending RequestVote to server %d failed\n", rf.me, ii)
			}
		}(int32(i))
	}
}

func (rf *RaftHTTPService) sendRequestVote(server int32, request *raft_api.RequestVoteRequest) (*raft_api.RequestVoteResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	response, err := rf.peers[server].RequestVote(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

func (rf *RaftHTTPService) convertToFollower(term int32, voteFor int32) {
	rf.currentTerm = term
	rf.state = Follower
	rf.totalVotes = 0
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *RaftHTTPService) HeartBeatChannel(args *raft_api.HeartBeadRequest) *raft_api.HeartBeadResponse {
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

func (rf *RaftHTTPService) SendHeartBeatReply(server int32, request *raft_api.HeartBeadRequest) (*raft_api.HeartBeadResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	response, err := rf.peers[server].HeartBead(ctx, request)
	if err == nil {
		return response, true
	} else {
		return response, false
	}
}

//func (rf *Raft) Write(args *Write, reply *WriteReply) {
//	rf.mu.Lock()
//	loga := Entry{
//		Term:    rf.currentTerm,
//		Command: args,
//	}
//	rf.log = append(rf.log, loga)
//	rf.seq_id++
//	rf.mu.Unlock()
//}
//
//func (rf *Raft) Read(args *Read, reply *ReadReply) {
//
//}

func (rf *RaftHTTPService) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *RaftHTTPService) GetOpLogs() (int, error) {
	_, err := rf.mongoclient.GetOplog()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return 1, nil
}

func (rf *RaftHTTPService) convertToLeader() {
	//may be election again
	rf.state = Leader
}

func (rf *RaftHTTPService) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *RaftHTTPService) setGrantVoteCh() {
	go func() {
		select {
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *RaftHTTPService) setLeaderCh() {
	go func() {
		select {
		case <-rf.leaderCh:
		default:
		}
		rf.leaderCh <- true
	}()
}

func (rf *RaftHTTPService) drainOldTimer() {
	select {
	case <-rf.timer.C:
		DPrintf("Server %d: drain the old timer\n", rf.me)
	default:
	}
}

func (rf *RaftHTTPService) RequestVote(ctx context.Context, in *raft_api.RequestVoteRequest) (*raft_api.RequestVoteResponse, error) {
	return rf.RequestVoteChannel(in), nil
}

func (rf *RaftHTTPService) HeartBead(ctx context.Context, in *raft_api.HeartBeadRequest) (*raft_api.HeartBeadResponse, error) {
	return rf.HeartBeatChannel(in), nil
}
