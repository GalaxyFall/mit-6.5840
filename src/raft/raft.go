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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type memberRole int

const (
	Leader memberRole = iota
	Follower
	Candidate
)

func (r memberRole) string() string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	}
	return "unknown"
}

const (
	None = -1
	// HeartbeatInterval must less than election rand time prevent
	// network delay or occasional loss of heartbeat will not trigger an election.
	defaultHeartbeatInterval = 50 * time.Millisecond
	electionMinRandTime      = 150
	electionMaxRandTime      = 350
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//election
	term             int
	voteFor          int
	role             memberRole
	nextElectionTime time.Time

	logs []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.term
	isleader = rf.role == Leader

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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int //record whose candidate request for vote
	LastLogTerm  int //candidate the last log entry term
	LastLogIndex int //candidate the last log entry index
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term  int  //reply curr node term to reject expired request
	Voted bool //curr node whether voted
}

// include heartbeat

type AppendEntriesArgs struct {
	Term      int
	LeaderId  int  //so follower can redirect clients
	HeartBeat bool //whether leader heartbeat rpc request
}

type AppendEntriesReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("node[%d->%s->%d] receive vote id[%d] term[%d] logTerm[%d] logIndex[%d]",
		rf.me, rf.role.string(), rf.term, args.CandidateID, args.Term, args.LastLogTerm, args.LastLogIndex)
	defer func() {
		DPrintf("node[%d->%s->%d] reply vote id[%d] resp %v", rf.me, rf.role.string(), rf.term, args.CandidateID, reply)
	}()

	//term greater curr reset follower but not return
	if args.Term > rf.term {
		rf.changeFollower(args.Term)
	}

	lastTerm, lastIndex := rf.getLastLogInfo()
	uptodate := args.LastLogTerm > lastTerm ||
		args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex

	if args.Term < rf.term {
		//reject expired request
		reply.Voted = false
	} else if (rf.voteFor != None && rf.voteFor == args.CandidateID) ||
		(rf.voteFor == None && uptodate) {
		//valid voted need reset time
		reply.Voted = true
		rf.voteFor = args.CandidateID
		rf.resetNextElectionTime()
		rf.persist()
	} else {
		reply.Voted = false
	}

	//note:cannot re-initialize one reply, always return new term
	reply.Term = rf.term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer func() {
		//note:cannot re-initialize one reply, return new term if became follower
		reply.Term = rf.term
		rf.mu.Unlock()
	}()

	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.changeFollower(args.Term)
	}

	//valid heartbeat reset time
	if args.HeartBeat {
		rf.resetNextElectionTime()
		return
	}

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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

// [150,350]
func (rf *Raft) randElectionTimeout() int64 {
	return electionMinRandTime + (rand.Int63() % (electionMaxRandTime - electionMinRandTime))
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.tick()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		rf.resetNextElectionTime()
		return
	}

	if rf.activate() {
		return
	}

	rf.startElection()
}

// resetNextElectionTime : election timeout period of each node is random
func (rf *Raft) resetNextElectionTime() {
	ms := rf.randElectionTimeout()
	rf.nextElectionTime = time.Now().Add(time.Millisecond * time.Duration(ms))
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rand.Seed(time.Now().UnixNano() + int64(me))
	rf.voteFor = None
	rf.role = Follower
	rf.term = 0
	rf.resetNextElectionTime()
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}

func (rf *Raft) getLastLogInfo() (term int, index int) {
	if len(rf.logs) == 0 {
		return 0, 0
	}

	return rf.logs[len(rf.logs)-1].Term, len(rf.logs) - 1
}

func (rf *Raft) isRole(role memberRole) bool {
	return rf.role == role
}

func (rf *Raft) changeLeader() {
	rf.role = Leader
	rf.voteFor = rf.me //note: voted myself

	go rf.heartbeatCall() //became leader right now send one heartbeat

	rf.persist()
}

func (rf *Raft) changeFollower(term int) {
	rf.role = Follower
	rf.term = term
	rf.voteFor = None
	// receive greater term became follower reset time
	rf.resetNextElectionTime()

	rf.persist()
}

func (rf *Raft) changeCandidate() {
	rf.role = Candidate
	rf.term++
	rf.voteFor = rf.me //note: voted myself
	// start next election reset time
	rf.resetNextElectionTime()

	rf.persist()
}

func (rf *Raft) leaderHandler() {
	DPrintf("leaderHandler [%d->%s->%d] ...", rf.me, rf.role.string(), rf.term)
}

// activate: check whether now need start next term
func (rf *Raft) activate() bool {
	return time.Now().Before(rf.nextElectionTime)
}

// startElection: election time activate,became candidate start vote request
func (rf *Raft) startElection() {
	rf.changeCandidate()

	rf.voteCall()
}

// voteCall: candidate call vote request rpc, note without lock call rpc
func (rf *Raft) voteCall() {

	lastTerm, lastIndex := rf.getLastLogInfo()

	request := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}

	votedCount := 1 //voted myself

	DPrintf("node[%d->%s] voteCall term %d", rf.me, rf.role.string(), rf.term)

	for serverId := range rf.peers {
		//skip curr node
		if serverId == rf.me {
			continue
		}
		go func(id int, req *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(id, req, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.term {
					rf.changeFollower(reply.Term)
					return
				}

				//maybe had been change role
				if !rf.isRole(Candidate) {
					return
				}

				//note: equal term voted is valid
				if reply.Voted && reply.Term == rf.term {
					votedCount++
				}

				//It's still term
				if votedCount > len(rf.peers)/2 && rf.term == req.Term {
					rf.changeLeader()
					return
				}
			}
		}(serverId, request)
	}

}

func (rf *Raft) heartbeatCall() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}

	request := &AppendEntriesArgs{
		Term:      rf.term,
		LeaderId:  rf.me,
		HeartBeat: true,
	}

	for serverId := range rf.peers {
		//skip curr node
		if serverId == rf.me {
			rf.resetNextElectionTime()
			continue
		}

		go func(id int, req *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(id, req, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				return
			}
			if reply.Term > rf.term {
				rf.changeFollower(reply.Term)
				return
			}
		}(serverId, request)
	}
}

// heartbeatTicker : leader ticker send heartbeat to keepalive
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.heartbeatCall()
		time.Sleep(defaultHeartbeatInterval)
	}
}
