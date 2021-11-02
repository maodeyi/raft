package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	labrpc2 "github.com/maodeyi/raft-demo/labrpc"
	"go.mongodb.org/mongo-driver/mongo"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "../labgob"

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
	Term     int
	LeaderId int
}

type HeartBeadReply struct {
	Term    int
	Success bool
}

const (
	Follower  string = "follower"
	Candidate        = "candidate"
	Leader           = "leader"
)

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex           // Lock to protect shared access to this peer's state
	peers       []*labrpc2.ClientEnd // RPC end points of all peers
	persister   *Persister           // Object to hold this peer's persisted state
	mongoclient *MongoClient
	me          int // this peer's index into peers[]
	//dead      int32                // set by Kill()
	syncdone bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	seq_id int
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
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
func (rf *Raft) persist() {
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
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.seq_id = len(rf.log)
	if data == nil || len(data) < 1 {
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	SeqId       int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
				if args.SeqId <= rf.seq_id {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
				} else {
					DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
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
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
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
	err := rf.mongoclient.InsertOpLog()
	if err != nil {
		DPrintf("mongo InsertOpLog error %v", err)
	}
	return rf.seq_id, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	//atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

//func (rf *Raft) killed() bool {
//	//z := atomic.LoadInt32(&rf.dead)
//	//return z == 1
//}

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
func Make(peers []*labrpc2.ClientEnd, me int,
	persister *Persister) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
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
	rf.subscribeOpLogs()

	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.subscribeOplogsCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.totalVotes = 0
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("--------------------- Resume server %d persistent state ---------------------\n", rf.me)

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
	return rf
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max-min) + min
	return randNum
}

func (rf *Raft) syncOpLogs() {
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

func (rf *Raft) subscribeOpLogs() {
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

func (rf *Raft) startHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf("Leader %d: start sending heartbeat, current term: %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			go func(ii int) {
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

					args := HeartBead{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := HeartBeadReply{}
					rf.mu.Unlock()
					ok := rf.SendHeartBeatReply(ii, &args, &reply)
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
			}(i)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	if rf.state != Candidate {
		DPrintf("no candiate")
		rf.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		SeqId:       rf.seq_id,
	}

	nLeader := 0
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		go func(ii int) {
			if ii == rf.me {
				return
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(ii, &args, &reply)
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
		}(i)
	}
}

func (rf *Raft) convertToFollower(term int, voteFor int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.totalVotes = 0
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *Raft) HeartBeat(args *HeartBead, reply *HeartBeadReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.setHeartBeatCh()
		rf.convertToFollower(args.Term, args.LeaderId)
	}
	DPrintf("======= server %d got HeartBeart from leader %d, args: %+v, current term: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.currentTerm, reply)

}

func (rf *Raft) SendHeartBeatReply(server int, args *HeartBead, reply *HeartBeadReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}

type Write struct{}

type WriteReply struct{}

type Read struct{}

type ReadReply struct{}

func (rf *Raft) Write(args *Write, reply *WriteReply) {
	rf.mu.Lock()
	loga := Entry{
		Term:    rf.currentTerm,
		Command: args,
	}
	rf.log = append(rf.log, loga)
	rf.seq_id++
	rf.mu.Unlock()
}

func (rf *Raft) Read(args *Read, reply *ReadReply) {

}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *Raft) GetOpLogs() (int, error) {
	_, err := rf.mongoclient.GetOplog()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return 1, nil
}

func (rf *Raft) convertToLeader() {
	//may be election again
	rf.state = Leader
}

func (rf *Raft) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *Raft) setGrantVoteCh() {
	go func() {
		select {
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *Raft) setLeaderCh() {
	go func() {
		select {
		case <-rf.leaderCh:
		default:
		}
		rf.leaderCh <- true
	}()
}

func (rf *Raft) drainOldTimer() {
	select {
	case <-rf.timer.C:
		DPrintf("Server %d: drain the old timer\n", rf.me)
	default:
	}
}
