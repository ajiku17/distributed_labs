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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"

import "bytes"
import "../labgob"

const (
	STATE_FOLLOWER  = 1
	STATE_CANDIDATE = 2
	STATE_LEADER    = 3
)

const (
	APPEND_ENTIRES_RPC = 1
	REQUEST_VOTE_RPC   = 2	
)

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

type LogEntry struct {
	Index 	int
	Term 	int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	currentState int
	currentTerm  int
	votedFor	 int
	commitIndex  int
	lastApplied  int

	log 	      []LogEntry
	lastHeartbeat time.Time

	// Leader state
	nextIndices			  []int
	matchIndices		  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.currentState == STATE_LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	DPrintf("Raft %d Term %d State %d saving state", rf.me, rf.currentTerm, rf.currentState)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	for _, v := range rf.log {
		e.Encode(v)
	}

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil {
	  DPrintf("Error while decoding")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	}

	var l LogEntry
	for {
		if err := d.Decode(&l); err == nil {
			rf.log = append(rf.log, l)
		} else {
			break
		}
	}
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		 int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("Raft %d Term %d State %d recieved call for request vote %v\n", rf.me, rf.currentTerm, rf.currentState, args)
	if args.Term < rf.currentTerm {
		DPrintf("Raft %d Term %d State %d old term for request vote", rf.me, rf.currentTerm, rf.currentState)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentState = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.logConsistency(args) {
		DPrintf("Raft %d Term %d State %d granting vote to %d", rf.me, rf.currentTerm, rf.currentState, args.CandidateID)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.currentState = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.lastHeartbeat = time.Now()
		DPrintf("Raft %d Term %d State %d updated last heartbeat time to %v", rf.me, rf.currentTerm, rf.currentState, rf.lastHeartbeat)
		rf.mu.Unlock()
		return
	}

	DPrintf("Raft %d Term %d State %d request vote inconsistency", rf.me, rf.currentTerm, rf.currentState)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("Raft %d Term %d State %d sending reply for request vote %v: %v\n", rf.me, rf.currentTerm, rf.currentState, args, reply)
	rf.mu.Unlock()
}


type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries 	 []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term				  int
	Success 			  bool
	ConflictingEntryIndex int
	ConflictingEntryTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("Raft %d Term %d State %d recieved call for append entries %v\n", rf.me, rf.currentTerm, rf.currentState, args)
	if args.Term < rf.currentTerm {
		DPrintf("Raft %d Term %d State %d refusing append entries %v because args.term %d < rf.currentTerm %d", rf.me, rf.currentTerm, rf.currentState, args, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("Raft %d Term %d State %d converting to follower", rf.me, rf.currentTerm, rf.currentState)
		rf.currentState = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.currentState == STATE_CANDIDATE {
		rf.currentState = STATE_FOLLOWER
	}

	rf.persist()

	found := -1
	prevTerm := -1
	for i, entry := range rf.log {
		if entry.Index == args.PrevLogIndex {
			found = i;
			prevTerm = entry.Term
			break;
		}
	}

	if !(args.PrevLogIndex == 0 && args.PrevLogTerm == 0) && 
			(found < 0 || args.PrevLogTerm != prevTerm) {
		DPrintf("Raft %d Term %d State %d refusing append entries %v from %d because of log inconsistency", rf.me, rf.currentTerm, rf.currentState, args, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false


		if len(rf.log) == 0 {
			reply.ConflictingEntryIndex = 0
			reply.ConflictingEntryTerm = 0
		} else {

			if prevTerm == -1 {
				reply.ConflictingEntryTerm = rf.log[len(rf.log) - 1].Term
			}

			for _, entry := range rf.log {
				if entry.Term == reply.ConflictingEntryTerm {
					reply.ConflictingEntryIndex = entry.Index	
					break;
				}
			}
		}
		rf.lastHeartbeat = time.Now()
		DPrintf("Raft %d Term %d State %d updated last heartbeat time to %v", rf.me, rf.currentTerm, rf.currentState, rf.lastHeartbeat)
		
		rf.mu.Unlock()
		return
	}
	
	DPrintf("Raft %d Term %d State %d accepting entries %v", rf.me, rf.currentTerm, rf.currentState, args.Entries)
	DPrintf("Raft %d Term %d State %d current log %v", rf.me, rf.currentTerm, rf.currentState, rf.log)
	newEntryIndex := 0
	for ; newEntryIndex < len(args.Entries); newEntryIndex++ {
		newEntry := args.Entries[newEntryIndex]
		localIndex, localTerm := rf.getLogInfo(newEntry.Index)
		if localIndex != newEntry.Index || localTerm != newEntry.Term {
			rf.truncateLogFrom(newEntry.Index)
			break
		}
	}

	DPrintf("Raft %d Term %d State %d current log %v", rf.me, rf.currentTerm, rf.currentState, rf.log)
	for newEntryIndex < len(args.Entries) {
		DPrintf("Raft %d Term %d State %d appended log entry %v", rf.me, rf.currentTerm, rf.currentState, args.Entries[newEntryIndex])
		rf.log = append(rf.log, args.Entries[newEntryIndex])
		newEntryIndex++
	}

	rf.persist()
	DPrintf("Raft %d Term %d State %d current log %v", rf.me, rf.currentTerm, rf.currentState, rf.log)


	if args.LeaderCommit > rf.commitIndex {
		DPrintf("Raft %d Term %d State %d recieved higher commit index %d from leader; old commit index %d", rf.me, rf.currentTerm, rf.currentState, args.LeaderCommit, rf.commitIndex)
		lastIndex, _ := rf.getLastLogInfo()
		if lastIndex < args.LeaderCommit {
			DPrintf("Raft %d Term %d State %d has shorter log than leaders commit setting commit index to %d", rf.me, rf.currentTerm, rf.currentState, lastIndex)
			rf.commitIndex = lastIndex
		} else {
			DPrintf("Raft %d Term %d State %d setting commit index to leaders commit %d", rf.me, rf.currentTerm, rf.currentState, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
		}
	}

	DPrintf("Raft %d Term %d State %d current log %v", rf.me, rf.currentTerm, rf.currentState, rf.log)

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastHeartbeat = time.Now()
	DPrintf("Raft %d Term %d State %d updated last heartbeat time to %v", rf.me, rf.currentTerm, rf.currentState, rf.lastHeartbeat)
	DPrintf("Raft %d Term %d State %d sending reply for appendEntries %v: %v\n", rf.me, rf.currentTerm, rf.currentState, args, reply)
	rf.mu.Unlock()	
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("Raft %d calling send request vote to raft %d", rf.me, server)

	numRetries := 3
	timeout := time.Duration(50)

	for i := 1; i <= numRetries; i++ {
		successChannel := make(chan RequestVoteReply, 1)
		go func (successChannel chan RequestVoteReply) {
			replyCopy := RequestVoteReply{}
			argsCopy := RequestVoteArgs{}
			argsCopy.CandidateID  = args.CandidateID
			argsCopy.Term 		  = args.Term
			argsCopy.LastLogIndex = args.LastLogIndex
			argsCopy.LastLogTerm  = args.LastLogTerm
			ok := rf.peers[server].Call("Raft.RequestVote", &argsCopy, &replyCopy)
			if ok {
				successChannel <- replyCopy
			}
		}(successChannel)

		timer := StartTimer(timeout);
		select {
		case rep := <- successChannel:
			DPrintf("Raft %d rpc call %v to %d succeeded on try %d", rf.me, "Raft.RequestVote", server, i)
			reply.Term = rep.Term
			reply.VoteGranted = rep.VoteGranted
			return true
		case <- timer:
			DPrintf("Raft %d failed to send rpc %v to %d on try %d", rf.me, "Raft.RequestVote", server, i)
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Raft %d calling send append entries to raft %d", rf.me, server)
	numRetries := 3
	timeout := time.Duration(50)

	for i := 1; i <= numRetries; i++ {
		successChannel := make(chan AppendEntriesReply, 1)
		go func (successChannel chan AppendEntriesReply) {
			replyCopy := AppendEntriesReply{}
			argsCopy := AppendEntriesArgs{}
			argsCopy.LeaderCommit = args.LeaderCommit
			argsCopy.LeaderId 	  = args.LeaderId
			argsCopy.PrevLogIndex = args.PrevLogIndex
			argsCopy.PrevLogTerm  = args.PrevLogTerm
			argsCopy.Term 		  = args.Term

			argsCopy.Entries = make([]LogEntry, len(args.Entries))
			for i, entry := range args.Entries {
				argsCopy.Entries[i] = entry
			}

			ok := rf.peers[server].Call("Raft.AppendEntries", &argsCopy, &replyCopy)
			if ok {
				successChannel <- replyCopy
			}
		}(successChannel)

		timer := StartTimer(timeout);
		select {
		case rep := <- successChannel:
			DPrintf("Raft %d rpc call %v to %d succeeded on try %d", rf.me, "Raft.AppendEntries", server, i)
			reply.Term = rep.Term
			reply.Success = rep.Success
			reply.ConflictingEntryIndex = rep.ConflictingEntryIndex
			reply.ConflictingEntryTerm = rep.ConflictingEntryTerm
			return true
		case <- timer:
			DPrintf("Raft %d failed to send rpc %v to %d on try %d", rf.me, "Raft.AppendEntries", server, i)
		}
	}
	return false
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != STATE_LEADER {
		return 0, 0, false
	}

	lastIndex, _ := rf.getLastLogInfo()
	newEntry := LogEntry {
		Index 	: lastIndex + 1,
		Term 	: rf.currentTerm,
		Command : command,
	}
	rf.log = append(rf.log, newEntry)
	index = newEntry.Index
	term = newEntry.Term
	isLeader = true
	rf.persist()

	DPrintf("Raft %d Term %d State %d leader agreed to replicate command %v\n Raft %d Term %d leader current log %v", rf.me, rf.currentTerm, rf.currentState, command, rf.me, rf.currentState, rf.log)

	return index, term, isLeader
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getCurrentState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentState
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}

	lastEntry := rf.log[len(rf.log) - 1]
	return lastEntry.Index, lastEntry.Term
}

func (rf *Raft) getLogInfo(index int) (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}

	if len(rf.log) < index {
		entry := rf.log[len(rf.log) - 1]
		return entry.Index, entry.Term
	}

	if index <= 0 {
		return 0, 0
	}

	entry := rf.log[index - 1]

	return entry.Index, entry.Term
}

func (rf *Raft) getLogsFrom(from int) []LogEntry {
	lastLogIndex, _ := rf.getLastLogInfo()
	
	if lastLogIndex >= from {
		if len(rf.log) == 0 {
			return []LogEntry{}
		}

		res := make([]LogEntry, len(rf.log) - (from - 1))
		for i := from - 1; i < len(rf.log); i++ {
			res[i - (from - 1)] = rf.log[i]
		}
		return res
	}

	return []LogEntry{}
}

func (rf *Raft) truncateLogFrom(entryIndex int) {
	for i, entry := range rf.log {
		if entry.Index == entryIndex {
			rf.log = rf.log[:i]
			return
		}
	}
}

func (rf *Raft) prepareAppendEntriesArgs(peer int, args *AppendEntriesArgs) {
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	nextIndex := rf.nextIndices[peer]
	
	prevLogIndex, prevLogTerm := rf.getLogInfo(nextIndex - 1)
	
	args.Entries = rf.getLogsFrom(nextIndex)
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.LeaderCommit = rf.commitIndex
}

func (rf *Raft) startReplicator() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
	
		go func (peer int) {
			for {
				rf.mu.Lock()
				DPrintf("Raft %d Term %d State %d running replicator loop for raft %d", rf.me, rf.currentTerm, rf.currentState, peer)
				if rf.currentState != STATE_LEADER || rf.killed() {
					rf.mu.Unlock()
					return
				}
				DPrintf("Raft %d Term %d State %d sending append entries to raft %d", rf.me, rf.currentTerm, rf.currentState, peer)
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				rf.prepareAppendEntriesArgs(peer, &args)
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(peer, args, &reply)
				DPrintf("Raft %d received a reply %v:%v for append Entries to raft %d", rf.me, ok, reply, peer)

				if ok {
					rf.mu.Lock()
					DPrintf("Raft %d Term %d State %d received a reply %v:%v for append Entries to raft %d", rf.me, rf.currentTerm, rf.currentState, ok, reply, peer)
					if args.Term ==  rf.currentTerm {

						if reply.Term > rf.currentTerm {
							rf.currentState = STATE_FOLLOWER
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()

							rf.mu.Unlock()
							return
						}

						if reply.Success {
							if len(args.Entries) > 0 {
								rf.matchIndices[peer] = args.Entries[len(args.Entries) - 1].Index
								rf.nextIndices[peer] = args.Entries[len(args.Entries) - 1].Index + 1
								rf.recalculateCommitIndex()
								DPrintf("Raft %d Term %d State %d updated match indices 1 %v, next indices %v", rf.me, rf.currentTerm, rf.currentState, rf.matchIndices, rf.nextIndices)
							} else {
								rf.matchIndices[peer] = args.PrevLogIndex
								rf.nextIndices[peer] = args.PrevLogIndex + 1
								rf.recalculateCommitIndex()
								DPrintf("Raft %d Term %d State %d updated match indices 2 %v, next indices %v", rf.me, rf.currentTerm, rf.currentState, rf.matchIndices, rf.nextIndices)
							}
						} else {
							rf.nextIndices[peer] = reply.ConflictingEntryIndex;
							DPrintf("Raft %d Term %d State %d falling back for raft %d. next indices : %v", rf.me, rf.currentTerm, rf.currentState, peer, rf.nextIndices)
							if rf.nextIndices[peer] < 1 {
								rf.nextIndices[peer] = 1
							}
						}
					}

					if lastIndex, _ := rf.getLastLogInfo(); rf.nextIndices[peer] > lastIndex {
						DPrintf("Raft %d Term %d State %d sleeping replicator for raft %d", rf.me, rf.currentTerm, rf.currentState, peer)
						rf.mu.Unlock()
						time.Sleep(60 * time.Millisecond)
					} else {
						rf.mu.Unlock()
					}
					
				}
			}
		}(i)
	}
}

func (rf *Raft) requestMajority() {
	DPrintf("Raft %d Term %d State %d running candidate", rf.me, rf.currentTerm, rf.currentState)
	rf.currentState = STATE_CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	voteCount := 1
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func (peer, term, lastLogIndex, lastLogTerm int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}

			rf.mu.Lock()
			args.Term = term
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			args.CandidateID = rf.me
			DPrintf("Raft %d Term %d State %d sending request vote to %d, current log %v", rf.me, rf.currentTerm, rf.currentState, peer, rf.log)
			rf.mu.Unlock()

			ok := rf.sendRequestVote(peer, args, &reply)
			DPrintf("Raft %d received a reply %v for request vote", rf.me, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			DPrintf("Raft %d Term %d State %d received a reply %v for request vote", rf.me, rf.currentTerm, rf.currentState, reply)
			if args.Term == rf.currentTerm {
				DPrintf("Raft %d Term %d State %d received a reply for a request vote from %d: %v", rf.me, rf.currentTerm, rf.currentState, peer, reply)
				if reply.Term > rf.currentTerm {
					DPrintf("Raft %d Term %d State %d stepping down to follower", rf.me, rf.currentTerm, rf.currentState)
					rf.currentState = STATE_FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
				} else if reply.VoteGranted {
					voteCount++;
					DPrintf("Raft %d Term %d State %d majority needed %d votes recieved %d", rf.me, rf.currentTerm, rf.currentState, (len(rf.peers) / 2) + 1, voteCount)
					if voteCount >= (len(rf.peers) / 2) + 1 {
						if rf.currentState == STATE_CANDIDATE {
							DPrintf("Raft %d Term %d State %d candidate was promoted to leader", rf.me, rf.currentTerm, rf.currentState)
							rf.currentState = STATE_LEADER
							rf.votedFor = -1
							rf.persist()
							rf.initReplicatorState()
							rf.startReplicator()
						} else {
							DPrintf("Raft %d Term %d State %d is no longer a candidate, it is now a %d", rf.me, rf.currentTerm, rf.currentState, rf.currentState)
						}
					}
				}
			}

			rf.mu.Unlock()
		}(i, term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) recalculateCommitIndex() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		entry := rf.log[i]
		if entry.Term == rf.currentTerm && entry.Index > rf.commitIndex {
			numMatched := 1
			for i, m := range rf.matchIndices {
				if i != rf.me && m >= entry.Index {
					numMatched++
				}
			}
			if numMatched >= (len(rf.peers) / 2) + 1 {
				DPrintf("Raft %d Term %d State %d updating commit index from %d to %d", rf.me, rf.currentTerm, rf.currentState, rf.commitIndex, entry.Index)
				rf.commitIndex = entry.Index
				return
			}
		}

	}
}

func (rf *Raft) initReplicatorState() {
	rf.nextIndices = make([]int, len(rf.peers))
	rf.matchIndices = make([]int, len(rf.peers))
	lastLogIndex, _ := rf.getLastLogInfo()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndices[i] = lastLogIndex + 1 
		rf.matchIndices[i] = 0
	}
}

func (rf *Raft) logConsistency(args *RequestVoteArgs) bool {
	lastIndex, lastTerm := rf.getLastLogInfo()

	if lastTerm == args.LastLogTerm {
		return lastIndex <= args.LastLogIndex 
	}

	return lastTerm < args.LastLogTerm
}

func (rf *Raft) applier() {
	DPrintf("Raft %d Term %d State %d applier going live", rf.me, rf.currentTerm, rf.currentState)
	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			DPrintf("Raft %d Term %d State %d last applied = %d commit index = %d", rf.me, rf.currentTerm, rf.currentState, rf.lastApplied, rf.commitIndex)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				DPrintf("Raft %d Term %d State %d Trying to apply log with index : %d, log index %d current log %v", rf.me, rf.currentTerm, rf.currentState, i, i - 1, rf.log)
				entry := rf.log[i - 1]
				rf.applyCh <- ApplyMsg {
					CommandValid : true,
					Command 	 : entry.Command,
					CommandIndex : entry.Index,
				}
				rf.lastApplied = entry.Index
			}
			rf.mu.Unlock()
		} else {
			DPrintf("Raft %d Term %d State %d does not have anything to apply commitIndex = %d, lastApplied = %d", rf.me, rf.currentTerm, rf.currentState, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()
			time.Sleep(25 * time.Millisecond)
		}
	}
}

func (rf *Raft) live() {
	DPrintf("Raft %d main loop", rf.me)

	timeout := RandomTimeoutDuration(200, 500) * time.Millisecond
	DPrintf("Raft %d now %v timeout %d future %v", rf.me, time.Now(), timeout, time.Now().Add(timeout))
	now := <- time.After(timeout)

	rf.mu.Lock()
	if !rf.killed() {
		if rf.currentState != STATE_LEADER && now.Sub(rf.lastHeartbeat) >= timeout  {
			DPrintf("Raft %d Term %d State %d  now %v heartbeat %v diff %v timeout %d", rf.me, rf.currentTerm, rf.currentState, now, rf.lastHeartbeat, now.Sub(rf.lastHeartbeat), timeout)
			rf.requestMajority()
		}
		go rf.live()
	}

	rf.mu.Unlock()	
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentState = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Raft %d Term %d State %d starting, state: %v", rf.me, rf.currentTerm, rf.currentState, rf)

	go rf.applier()
	go rf.live()

	return rf
}
