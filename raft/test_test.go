package raft

//
////
//// Raft tests.
////
//// we will use the original test_test.go to test your code for grading.
//// so, while you can modify this code to help you debug, please
//// test with the original before submitting.
////
//
//import (
//	"fmt"
//	"math/rand"
//	"testing"
//)
//import "time"
//
//// The tester generously allows solutions to complete elections in one second
//// (much more than the paper's range of timeouts).
//const RaftElectionTimeout = 1000 * time.Millisecond
//
//func TestInitialElection2A(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2A): initial election")
//
//	// is a leader elected?
//	cfg.checkOneLeader()
//
//	// sleep a bit to avoid racing with followers learning of the
//	// election, then check that all peers agree on the term.
//	time.Sleep(200 * time.Millisecond)
//	term1 := cfg.checkTerms()
//	if term1 < 1 {
//		t.Fatalf("term is %v, but should be at least 1", term1)
//	}
//
//	// does the leader+term stay the same if there is no network failure?
//	time.Sleep(2 * RaftElectionTimeout)
//	term2 := cfg.checkTerms()
//	if term1 != term2 {
//		fmt.Printf("warning: term changed even though there were no failures")
//	}
//
//	// there should still be a leader.
//	cfg.checkOneLeader()
//
//	cfg.end()
//}
//
//func TestReElection2A(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2A): election after network failure")
//
//	leader1 := cfg.checkOneLeader()
//
//	// if the leader disconnects, a new one should be elected.
//	cfg.disconnect(leader1)
//	cfg.checkOneLeader()
//	// if the old leader rejoins, that shouldn't
//	// disturb the new leader.
//	cfg.connect(leader1)
//	leader2 := cfg.checkOneLeader()
//
//	// if there's no quorum, no leader should
//	// be elected.
//	cfg.disconnect(leader2)
//	cfg.disconnect((leader2 + 1) % servers)
//	time.Sleep(2 * RaftElectionTimeout)
//	cfg.checkNoLeader()
//
//	// if a quorum arises, it should elect a leader.
//	cfg.connect((leader2 + 1) % servers)
//	cfg.checkOneLeader()
//
//	// re-join of last node shouldn't prevent leader from existing.
//	cfg.connect(leader2)
//	cfg.checkOneLeader()
//
//	cfg.end()
//}
//
//func TestFailAgree2B(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): agreement despite follower disconnection")
//
//	cfg.one(101, servers, false)
//
//	// disconnect one follower from the network.
//	leader := cfg.checkOneLeader()
//	cfg.disconnect((leader + 1) % servers)
//
//	// the leader and remaining follower should be
//	// able to agree despite the disconnected follower.
//	cfg.one(102, servers-1, false)
//	cfg.one(103, servers-1, false)
//	time.Sleep(RaftElectionTimeout)
//	cfg.one(104, servers-1, false)
//	cfg.one(105, servers-1, false)
//
//	// re-connect
//	cfg.connect((leader + 1) % servers)
//
//	// the full set of servers should preserve
//	// previous agreements, and be able to agree
//	// on new commands.
//	cfg.one(106, servers, true)
//	time.Sleep(RaftElectionTimeout)
//	cfg.one(107, servers, true)
//
//	cfg.end()
//}
//
//func TestFailNoAgree2B(t *testing.T) {
//	servers := 5
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): no agreement if too many followers disconnect")
//
//	cfg.one(10, servers, false)
//
//	// 3 of 5 followers disconnect
//	leader := cfg.checkOneLeader()
//	cfg.disconnect((leader + 1) % servers)
//	cfg.disconnect((leader + 2) % servers)
//	cfg.disconnect((leader + 3) % servers)
//
//	index, _, ok := cfg.rafts[leader].Start(20)
//	if ok != true {
//		t.Fatalf("leader rejected Start()")
//	}
//	if index != 1 {
//		t.Fatalf("expected index 1, got %v", index)
//	}
//
//	time.Sleep(2 * RaftElectionTimeout)
//
//	//n, _ := cfg.nCommitted(index)
//	//if n > 0 {
//	//	t.Fatalf("%v committed but no majority", n)
//	//}
//
//	// repair
//	cfg.connect((leader + 1) % servers)
//	cfg.connect((leader + 2) % servers)
//	cfg.connect((leader + 3) % servers)
//
//	// the disconnected majority may have chosen a leader from
//	// among their own ranks, forgetting index 2.
//	leader2 := cfg.checkOneLeader()
//	index2, _, ok2 := cfg.rafts[leader2].Start(30)
//	if ok2 == false {
//		t.Fatalf("leader2 rejected Start()")
//	}
//	if index2 < 1 || index2 > 2 {
//		t.Fatalf("unexpected index %v", index2)
//	}
//
//	cfg.one(1000, servers, true)
//
//	cfg.end()
//}
//
//func TestRejoin2B(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): rejoin of partitioned leader")
//
//	cfg.one(101, servers, true)
//
//	// leader network failure
//	leader1 := cfg.checkOneLeader()
//	cfg.disconnect(leader1)
//
//	// make old leader try to agree on some entries
//	cfg.rafts[leader1].Start(102)
//	cfg.rafts[leader1].Start(103)
//	cfg.rafts[leader1].Start(104)
//
//	// new leader commits, also for index=2
//	cfg.one(103, 2, true)
//
//	// new leader network failure
//	leader2 := cfg.checkOneLeader()
//	cfg.disconnect(leader2)
//
//	// old leader connected again
//	cfg.connect(leader1)
//
//	cfg.one(104, 2, true)
//
//	// all together now
//	cfg.connect(leader2)
//
//	cfg.one(105, servers, true)
//
//	cfg.end()
//}
//
//func TestBackup2B(t *testing.T) {
//	servers := 5
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")
//
//	cfg.one(rand.Int(), servers, true)
//
//	leader1 := cfg.checkOneLeader()
//	cfg.disconnect((leader1 + 2) % servers)
//	cfg.disconnect((leader1 + 3) % servers)
//	cfg.disconnect((leader1 + 4) % servers)
//
//	for i := 0; i < 50; i++ {
//		cfg.rafts[leader1].Start(rand.Int())
//	}
//
//	time.Sleep(RaftElectionTimeout / 2)
//
//	cfg.disconnect((leader1 + 0) % servers)
//	cfg.disconnect((leader1 + 1) % servers)
//
//	cfg.connect((leader1 + 2) % servers)
//	cfg.connect((leader1 + 3) % servers)
//	cfg.connect((leader1 + 4) % servers)
//
//	for i := 0; i < 50; i++ {
//		cfg.one(rand.Int(), 3, true)
//	}
//
//	leader2 := cfg.checkOneLeader()
//	other := (leader1 + 2) % servers
//	if leader2 == other {
//		other = (leader2 + 1) % servers
//	}
//	cfg.disconnect(other)
//
//	for i := 0; i < 50; i++ {
//		cfg.rafts[leader2].Start(rand.Int())
//	}
//
//	time.Sleep(RaftElectionTimeout / 2)
//
//	// bring original leader back to life,
//	for i := 0; i < servers; i++ {
//		cfg.disconnect(i)
//	}
//	cfg.connect((leader1 + 0) % servers)
//	cfg.connect((leader1 + 1) % servers)
//	cfg.connect(other)
//
//	for i := 0; i < 50; i++ {
//		cfg.one(rand.Int(), 3, true)
//	}
//
//	for i := 0; i < servers; i++ {
//		cfg.connect(i)
//	}
//	cfg.one(rand.Int(), servers, true)
//
//	cfg.end()
//}
