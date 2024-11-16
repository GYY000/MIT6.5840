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
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sort"
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

const LeaderCon = 2
const CandidateCon = 1
const FollowerCon = 0
const CheckTime = 200
const MaxTimeout = 1000
const MinTimeout = 500
const HeartBeatTime = 100

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurrentTerm int
	VotedFor    int       // -1 means nil haven't voted for
	identity    int       // 0 for followers,1 for candidates,2 for leader
	leaderId    int       // use for find leader
	timestamp   time.Time // record last time hear from others

	getVote int
	Log     []Entry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	applyCh chan ApplyMsg

	applySync  *sync.Cond
	commitSync *sync.Cond
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
	isleader = rf.identity == LeaderCon
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	DPrintf("save %d %d %d\n", rf.me, rf.CurrentTerm, rf.VotedFor)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("error decode\n")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		DPrintf("load %d %d %d\n", rf.me, rf.CurrentTerm, rf.VotedFor)
		rf.mu.Unlock()
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
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock() // to protect CurrentTerm
	defer rf.mu.Unlock()
	DPrintf("%d receive request vote from %d\n", rf.me, args.CandidateId)
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
	} else {
		if rf.CurrentTerm < args.Term {
			rf.updateTerm(args.Term)
		}
		lastTerm, lastIndex := rf.getLastLogInfo()
		//avoid replicate vote
		if (rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) || args.LastLogTerm < lastTerm ||
			(args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
			reply.VoteGranted = false
		} else {
			DPrintf("%d vote for %d\n", rf.me, args.CandidateId)
			rf.timestamp = time.Now()
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.persist()
		}
	}
	reply.Term = rf.CurrentTerm
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
	DPrintf("%d send request to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
	FIndex  int
	FTerm   int
	FLen    int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.FLen = len(rf.Log)
	DPrintf("%d:receive append entry from %d\n", rf.me, args.LeaderId)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	rf.timestamp = time.Now()
	if rf.CurrentTerm < args.Term {
		rf.updateTerm(args.Term)
		rf.leaderId = args.LeaderId
	}
	if args.PrevLogIndex != -1 && (len(rf.Log) <= args.PrevLogIndex ||
		rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		DPrintf("follower %d find confilict at %d\n", rf.me, args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		if len(rf.Log) > args.PrevLogIndex {
			xIndex := args.PrevLogIndex
			reply.FTerm = rf.Log[args.PrevLogIndex].Term
			for xIndex >= 0 && rf.Log[xIndex].Term == reply.FTerm {
				xIndex--
			}
			xIndex++
			reply.FIndex = xIndex
		}
		rf.mu.Unlock()
		DPrintf("follower return_back %d findex:%d fterm: %d, flen: %d\n", rf.me, reply.FIndex, reply.FTerm, reply.FLen)
		return
	}
	//find the first different entry and then remake the log
	if len(args.Entries) != 0 {
		i := args.PrevLogIndex + 1
		j := 0
		for ; i < len(rf.Log) && j < len(args.Entries); i++ {
			if rf.Log[i].Term != args.Entries[j].Term {
				break
			}
			j++
		}
		rf.Log = append(rf.Log[:i], args.Entries[j:]...)
		reply.FLen = len(rf.Log)
		rf.persist()
		DPrintf("follower %d get entries new to %d\n", rf.me, len(rf.Log)-1)
	} else {
		DPrintf("---WARNING---Leader %d TermLeader:%d Follower:%d Termfollower %d get entries new to %d\n",
			args.LeaderId, args.Term, rf.me, rf.CurrentTerm, len(rf.Log)-1)
	}
	reply.Term = rf.CurrentTerm
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit > len(rf.Log)-1 {
			rf.CommitIndex = len(rf.Log) - 1
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.mu.Unlock()
		reply.Success = true
		rf.applySync.L.Lock()
		rf.applySync.Signal()
		rf.applySync.L.Unlock()
		return
	}
	rf.mu.Unlock()
	reply.Success = true
	return
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
	index := -1
	term := -1
	isLeader := rf.identity == LeaderCon
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.CurrentTerm
	rf.Log = append(rf.Log, Entry{
		Term:    rf.CurrentTerm,
		Command: command,
	})
	rf.persist()
	index = len(rf.Log)
	DPrintf("leader %d append log %d, Term %d\n", rf.me, index, term)
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
	DPrintf("%d is killed--------------------\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.Log) > 0 {
		return rf.Log[len(rf.Log)-1].Term, len(rf.Log) - 1
	} else {
		return -1, -1
	}
}

func (rf *Raft) getRandTimeOut() int64 {
	r := rand.New(rand.NewSource(int64(rf.me)))
	return int64(r.Int31n(MaxTimeout-MinTimeout) + MinTimeout)
}

func (rf *Raft) leaderInit() {
	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = -1
	}
}

func (rf *Raft) genEntryServer() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.appendServer(i)
	}
}

func (rf *Raft) appendServer(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.identity != LeaderCon {
			rf.mu.Unlock()
			break
		}
		if len(rf.Log) > rf.NextIndex[server] {
			reply := AppendEntriesReply{
				Term:    0,
				Success: false,
				FIndex:  -1,
				FLen:    -1,
				FTerm:   -1,
			}
			prevLogTerm := -1
			if rf.NextIndex[server] > 0 {
				prevLogTerm = rf.Log[rf.NextIndex[server]-1].Term
			}
			temp_term := rf.CurrentTerm
			entries := rf.Log[rf.NextIndex[server]:]
			max_index := len(rf.Log) - 1
			CommitIndex := rf.CommitIndex
			prevLogIndex := rf.NextIndex[server] - 1
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &AppendEntriesArgs{
				Term:         temp_term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: CommitIndex,
			}, &reply)
			rf.mu.Lock()
			if ok && reply.Term > rf.CurrentTerm {
				rf.updateTerm(reply.Term)
				rf.timestamp = time.Now()
			}
			if ok && reply.Success {
				DPrintf("update follower %d matchIndex %d\n", server, max_index)
				rf.MatchIndex[server] = reply.FLen - 1
				rf.NextIndex[server] = reply.FLen
				DPrintf("0:leader:%d update server:%d nextIndex to %d with LogLen %d\n", rf.me, server, rf.NextIndex[server], len(rf.Log))
				rf.mu.Unlock()
				rf.commitSync.L.Lock()
				rf.commitSync.Broadcast()
				rf.commitSync.L.Unlock()
			} else if ok && reply.FLen >= 0 {
				DPrintf("follower return %d findex:%d fterm: %d, flen: %d\n", server, reply.FIndex, reply.FTerm, reply.FLen)
				if reply.FLen <= prevLogIndex {
					rf.NextIndex[server] = reply.FLen
					DPrintf("1:leader:%d update server:%d nextIndex to %d with LogLen %d\n", rf.me, server, rf.NextIndex[server], len(rf.Log))
				} else {
					temp := prevLogIndex
					for temp >= 0 && rf.Log[temp].Term > reply.FTerm {
						temp--
					}
					if temp == -1 {
						rf.NextIndex[server] = 0
					} else {
						if rf.Log[temp].Term < reply.FTerm {
							rf.NextIndex[server] = reply.FIndex
							DPrintf("2:leader:%d update server:%d nextIndex to %d with LogLen %d\n", rf.me, server, rf.NextIndex[server], len(rf.Log))
						} else {
							rf.NextIndex[server] = temp
							DPrintf("3:leader:%d update server:%d nextIndex to %d with LogLen %d\n", rf.me, server, rf.NextIndex[server], len(rf.Log))
						}
					}
				}
				//rf.NextIndex[server]--
				rf.mu.Unlock()
				continue
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) elect() {
	// log to keep the server to be
	DPrintf("%d:start elect\n", rf.me)
	rf.mu.Lock()
	rf.getVote = 1
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{
			Term:        0,
			VoteGranted: false,
		}
		lastTerm, lastIndex := rf.getLastLogInfo()
		go func(x int) {
			// check if ok have to be in a loop
			ok := rf.sendRequestVote(x,
				&RequestVoteArgs{CandidateId: rf.me, Term: rf.CurrentTerm, LastLogTerm: lastTerm, LastLogIndex: lastIndex},
				&reply)
			rf.mu.Lock()
			if ok && reply.Term > rf.CurrentTerm {
				rf.updateTerm(reply.Term)
				rf.timestamp = time.Now()
				rf.mu.Unlock()
				return
			}
			if ok && reply.VoteGranted && rf.identity == CandidateCon {
				rf.getVote++
				if rf.getVote > len(rf.peers)/2 {
					rf.identity = LeaderCon
					DPrintf("%d become leader\n", rf.me)
					rf.leaderInit()
					rf.mu.Unlock()
					go rf.heartBeat()
					go rf.genEntryServer()
					go rf.commitTicker()
					return
				}
			}
			rf.mu.Unlock()
		}(i)
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateTerm(term int) {
	rf.identity = FollowerCon
	rf.VotedFor = -1
	rf.CurrentTerm = term
	rf.getVote = 0
	rf.persist()
}

func (rf *Raft) heartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.heartBeatServer(i)
	}
}

func (rf *Raft) heartBeatServer(server int) {
	for rf.killed() == false {
		rf.mu.Lock() // log to keep the server to be
		if rf.identity != LeaderCon {
			rf.mu.Unlock()
			return
		}
		DPrintf("%d:send Heartbeat\n", rf.me)
		reply := AppendEntriesReply{
			Term:    0,
			Success: false,
			FTerm:   -1,
			FLen:    -1,
			FIndex:  -1,
		}
		prevLogTerm := -1
		if rf.NextIndex[server] > 0 {
			DPrintf("nextIndex:%d %d nextIndex[x]: %d\n", rf.me, server, rf.NextIndex[server])
			prevLogTerm = rf.Log[rf.NextIndex[server]-1].Term
		}
		prevLogIndex := rf.NextIndex[server] - 1
		CommitIndex := rf.CommitIndex
		tempTerm := rf.CurrentTerm
		rf.mu.Unlock()

		rf.sendAppendEntries(server, &AppendEntriesArgs{tempTerm, rf.me, prevLogIndex,
			prevLogTerm, make([]Entry, 0), CommitIndex},
			&reply)
		rf.mu.Lock()
		if rf.CurrentTerm < reply.Term {
			rf.updateTerm(reply.Term)
			rf.timestamp = time.Now()
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatTime * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.timestamp).Milliseconds() > rf.getRandTimeOut() && rf.identity != LeaderCon {
			rf.identity = CandidateCon
			go rf.elect()
		}
		rf.mu.Unlock()
		time.Sleep(CheckTime * time.Millisecond) // check whether time out
	}
}

func (rf *Raft) commitTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.identity != LeaderCon {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		rf.commitSync.L.Lock()
		for {
			rf.commitSync.Wait()
			rf.mu.Lock()
			rf.MatchIndex[rf.me] = len(rf.Log) - 1
			b := make([]int, len(rf.MatchIndex))
			copy(b, rf.MatchIndex)
			sort.Ints(b)
			element := b[(len(b)-1)/2]
			if rf.CommitIndex < element && rf.Log[element].Term == rf.CurrentTerm {
				DPrintf("Leader %d update commitIndex %d to %d\n", rf.me, rf.CommitIndex, element)
				rf.CommitIndex = element
				rf.mu.Unlock()
				rf.applySync.L.Lock()
				rf.applySync.Broadcast()
				rf.applySync.L.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		rf.commitSync.L.Unlock()
	}
}

// timely apply committed command
func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		rf.applySync.L.Lock()
		for rf.CommitIndex <= rf.LastApplied {
			rf.applySync.Wait()
		}
		rf.applySync.L.Unlock()
		for {
			rf.mu.Lock()
			if rf.CommitIndex <= rf.LastApplied {
				rf.mu.Unlock()
				break
			}
			rf.LastApplied++
			apply_index := rf.LastApplied + 1
			DPrintf("lastApplied:%d %d\n", rf.me, rf.LastApplied)
			command := rf.Log[rf.LastApplied].Command
			rf.mu.Unlock()
			DPrintf("%d apply index %d, term %d\n", rf.me, apply_index, rf.Log[rf.LastApplied].Term)
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  apply_index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			DPrintf("%d apply index %d, term %d successfully\n", rf.me, apply_index, rf.Log[rf.LastApplied].Term)
		}
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.identity = FollowerCon
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.timestamp = time.Now()
	rf.getVote = 0

	rf.applySync = sync.NewCond(&sync.Mutex{})
	rf.commitSync = sync.NewCond(&sync.Mutex{})

	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyTicker()

	return rf
}
