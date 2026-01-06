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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	follower State = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Condition variable to signal when new entries are committed
	cond *sync.Cond
	// Channel to trigger log replication immediately
	replicationTrigger chan bool
	// Election timer
	electionTimer *time.Timer

	// The server's current term
	currentTerm int
	// CandidateId that received vote in current term, or -1 if none
	votedFor int
	// All known log entries
	log []LogEntry

	// Index of highest log entry known to be committed
	commitIndex int
	// Index of highest log entry applied to state machine
	lastApplied int

	// Contains the next log index to send to each server
	nextIndex []int
	// Contains the highest log index known to be replicated on each server
	matchIndex []int

	// Current state of the server.
	// follower, candidate or leader
	state State
}

type LogEntry struct {
	Command interface{}
	// Term when log entry was inserted
	Term int
}

// Helper function to reset election timer of this server
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(randomizedElectionTimeout())
}

func (rf *Raft) lastLog() (int, int) {
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < 0 {
		return -1, -1
	}
	return lastLogIndex, rf.log[lastLogIndex].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	candidateId := args.CandidateId
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = follower
	}

	lastLogIndex, lastLogTerm := rf.lastLog()
	if (rf.votedFor == -1 || rf.votedFor == candidateId) && logAhead(args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex) {
		rf.votedFor = candidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.resetElectionTimer()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func logAhead(term1, term2, index1, index2 int) bool {
	if term1 > term2 {
		return true
	} else if term1 == term2 && index1 >= index2 {
		return true
	}
	return false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = follower
		rf.resetElectionTimer()
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	// Trigger replication immediately unless there's already one ongoing.
	// This is how it's recommended in the Raft paper.
	// Although since we're restricted to 100 ms heartbeat interval,
	// this might break some tests.
	select {
	case rf.replicationTrigger <- true:
	default:
	}
	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// Sleep until there are new committed entries to apply.
		// This avoids busy waiting.
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleElectionTimeout() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.state != leader {
		rf.state = candidate
		rf.mu.Unlock()
		rf.startElection()
	} else {
		rf.mu.Unlock()
	}

	rf.resetElectionTimer()
}

func (rf *Raft) broadcastAppendEntries() {
	for !rf.killed() {
		if !rf.replicateLogToAllPeers() {
			return
		}

		select {
		case <-rf.replicationTrigger:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (rf *Raft) replicateLogToAllPeers() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return false
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateLogToPeer(peer)
		}
	}
	return true
}

func (rf *Raft) replicateLogToPeer(peer int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := rf.log[rf.nextIndex[peer]:]
	entriesCopy := make([]LogEntry, len(entries))
	copy(entriesCopy, entries)

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entriesCopy,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(peer, args, reply) {
		rf.handleAppendEntriesReply(peer, args, reply)
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Assert that we still are synchronized
	if rf.state != leader || rf.currentTerm != args.Term || rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	if !reply.Success {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			lastIndexOfTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
			}
			if lastIndexOfTerm != -1 {
				rf.nextIndex[peer] = lastIndexOfTerm + 1
			} else {
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		go rf.replicateLogToPeer(peer)
		return
	}
	rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
	rf.advanceCommitIndex()
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) advanceCommitIndex() {
	if rf.state != leader {
		return
	}
	N := len(rf.log) - 1
	for ; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2.0 {
			rf.commitIndex = N
			rf.cond.Broadcast()
			break
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.currentTerm++
	rf.votedFor = rf.me
	lastLogIndex, lastLogTerm := rf.lastLog()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := make(chan bool, len(rf.peers)-1)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.requestVoteFromPeer(peer, args, votes)
	}

	go rf.countVotes(votes, rf.currentTerm)
}

func (rf *Raft) requestVoteFromPeer(peer int, args *RequestVoteArgs, votes chan bool) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(peer, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = follower
			rf.resetElectionTimer()
		}
		rf.persist()
		rf.mu.Unlock()
		votes <- reply.VoteGranted
	} else {
		votes <- false
	}
}

func (rf *Raft) countVotes(votes chan bool, currentTerm int) {
	grantedVotes := 1
	peerCount := len(rf.peers)
	for i := 0; i < peerCount-1; i++ {
		voteGranted := <-votes
		if voteGranted {
			grantedVotes++
		}
		rf.mu.Lock()
		if rf.state == candidate && rf.currentTerm == currentTerm && grantedVotes > peerCount/2.0 {
			rf.becomeLeader()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	peerCount := len(rf.peers)
	rf.nextIndex = make([]int, peerCount)
	// Initialize volatile leader state according to paper
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, peerCount)
	go rf.broadcastAppendEntries()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.resetElectionTimer()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = follower
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.resetElectionTimer()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = follower
	}
	if args.Term >= rf.currentTerm {
		rf.resetElectionTimer()
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		return
	}
	// Log is shorter than PrevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	// Term mismatch at PrevLogIndex
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// Scan back to find the first index of this conflicting term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}
	// Log matches, proceed to append
	reply.Success = true
	matchLen := 0
	for i := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term == args.Entries[i].Term {
				matchLen++
			} else {
				rf.log = rf.log[:idx]
				break
			}
		}
	}
	if matchLen < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[matchLen:]...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Broadcast()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.replicationTrigger = make(chan bool, 1)
	rf.cond = sync.NewCond(&rf.mu)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Command: nil, Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initialize election timer
	rf.electionTimer = time.AfterFunc(randomizedElectionTimeout(), rf.handleElectionTimeout)
	go rf.applier()

	return rf
}

func randomizedElectionTimeout() time.Duration {
	// Since we're restricted to 100 ms heartbeat interval,
	// we set election timeout to be in the range of [500ms, 1000ms].
	// According to sources online the standard is 5x-10x of heartbeat interval.
	ms := 500 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
}
