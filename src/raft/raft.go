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
import "math/rand"
import "time"
import "log"
import "math"

// import "bytes"
// import "../labgob"

// Constant definitions
const (
	STATE_FOLLOWER = 1
	STATE_CANDIDATE = 2
	STATE_LEADER = 3
)

const (
	COMMAND_APPEND_ENTRIES = 1
	COMMAND_REQUEST_VOTE = 2
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


//
//	Command types
//
type requestVoteCommandRequest struct {
	term 	  	 int
	candidateID  int
}

type requestVoteCommandResponse struct {
	term 	  	int
	voteGranted bool
}

type appendEntriesCommandRequest struct {
	term 	 int
	leaderID int
}	

type appendEntriesCommandResponse struct {
	term int
}	

//
// A Go object to represent an rpc request
//
type CommandObj struct {
	CommandType int
	Command interface{}
}

type commandProcessPipe struct {
	requestedCommand CommandObj
	responseChannel  chan interface{}	
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
	currentState 		  int
	currentTerm     	  int
	commandRequestChannel chan commandProcessPipe
	votedFor			  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	term := rf.getTerm()
	isleader := rf.isLeader()

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
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 	  	 int
	CandidateID  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 	    int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term 	 int
	LeaderID int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	pipe := commandProcessPipe{}

	commandRequest := requestVoteCommandRequest{}
	commandRequest.term = args.Term
	commandRequest.candidateID = args.CandidateID

	command := CommandObj{}
	command.CommandType = COMMAND_REQUEST_VOTE
	command.Command = commandRequest
	
	pipe.responseChannel = make(chan interface{}, 1)
	pipe.requestedCommand = command

	rf.commandRequestChannel <- pipe
	
	// TODO: another level of indirection
	response := (<- pipe.responseChannel).(requestVoteCommandResponse)

	reply.Term = response.term
	reply.VoteGranted = response.voteGranted
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	pipe := commandProcessPipe{}

	commandRequest := appendEntriesCommandRequest{}
	commandRequest.term = args.Term
	commandRequest.leaderID = args.LeaderID

	command := CommandObj{}
	command.CommandType = COMMAND_APPEND_ENTRIES
	command.Command = commandRequest

	pipe.responseChannel = make(chan interface{}, 1)
	pipe.requestedCommand = command

	rf.commandRequestChannel <- pipe

	// TODO: another level of indirection
	response := (<- pipe.responseChannel).(appendEntriesCommandResponse)

	reply.Term = response.term
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

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

/*** Getters and Setters ***/

func (rf *Raft) getState() int {
	var state int
	rf.mu.Lock()
	state = rf.currentState
	rf.mu.Unlock()

	return state
}

func (rf *Raft) setState(newState int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentState = newState
}

func (rf *Raft) setVotedFor(value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = value
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.votedFor
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) setTerm(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = newTerm
}

func (rf *Raft) incrementTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
}

func (rf *Raft) isLeader() bool {
	currentState := rf.getState()

	return currentState == STATE_LEADER
}

func (rf *Raft) getMe() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.me
}

func (rf* Raft) getPeers() []*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peers[:]
}

// ***********

// TODO
func (rf* Raft) convertTo(state int, ) {
	oldState := rf.getState()
	rf.setState(state)
	rf.setVotedFor(-1)

	switch state {

	case STATE_FOLLOWER:
		
	case STATE_CANDIDATE:

	case STATE_LEADER:

	default:
		// For debuging purposes
		log.Printf("Raft %d: Conversion to unknown state - %d", rf.getMe(), state)
		rf.setState(oldState) 
	}

	
}

// **********

/*** Communication ***/

func (rf *Raft) generateTimeoutDuration(from, to int) time.Duration {
	return time.Duration(from + rand.Intn(to - from))
}

func (rf *Raft) processAppendEntries(request appendEntriesCommandRequest, responseChannel chan interface{}) bool {
	response := appendEntriesCommandResponse{}
	currentTerm := rf.getTerm()
	legalRequest := false


	if request.term < currentTerm {
		response.term = currentTerm
	} else {
		legalRequest = true
	}

	responseChannel <- response
	return legalRequest
}

func (rf *Raft) processRequestVote(request requestVoteCommandRequest, responseChannel chan interface{}) bool {
	response := requestVoteCommandResponse{}
	currentTerm := rf.getTerm()

	if request.term < currentTerm {
		response.term = currentTerm
		response.voteGranted = false
	} else {
		votedFor := rf.getVotedFor()
		if votedFor == -1 || votedFor == request.candidateID {
			response.term = currentTerm
			response.voteGranted = true
			rf.setVotedFor(request.candidateID)
		}
	}

	responseChannel <- response
	return response.voteGranted
}	

func (rf *Raft) processCommand(pipe commandProcessPipe) bool {
	command := pipe.requestedCommand
	currentTerm := rf.getTerm()
	resetTimer := false

	switch command.CommandType {

	case COMMAND_APPEND_ENTRIES:
		appendEntriesCommand := command.Command.(appendEntriesCommandRequest)
		if currentTerm < appendEntriesCommand.term {
			rf.convertTo(STATE_FOLLOWER)
			rf.setTerm(appendEntriesCommand.term)
		}
		resetTimer = rf.processAppendEntries(appendEntriesCommand, pipe.responseChannel)
	case COMMAND_REQUEST_VOTE:
		requestVoteCommand := command.Command.(requestVoteCommandRequest)
		if currentTerm < requestVoteCommand.term {
			rf.convertTo(STATE_FOLLOWER)
			rf.setTerm(requestVoteCommand.term)
		}
		resetTimer = rf.processRequestVote(requestVoteCommand, pipe.responseChannel)
	default:
		log.Printf("Raft %d: Unknown Command type %d - command %v\n", rf.getMe(), command.CommandType, command)
	}

	return resetTimer
}

func (rf *Raft) liveFollower() {
	resetTimer := true
	heartbeatTimeout := MakeTimer(rf.generateTimeoutDuration(1550, 1750))

	for rf.getState() == STATE_FOLLOWER {
		
		if resetTimer {
			heartbeatTimeout = MakeTimer(rf.generateTimeoutDuration(1550, 1750))
		}

		select {
		case _, ok := <- heartbeatTimeout:
			if !ok {
				log.Fatalf("Raft %d: Invalid timer follower", rf.getMe())
			}

			rf.convertTo(STATE_CANDIDATE)
			return
		case rpc := <- rf.commandRequestChannel:
			// TODO when should you reset the timer ?
			resetTimer = rf.processCommand(rpc)
		}

	}
}

func (rf* Raft) askForVote(peerIndex int, electionBucket chan int) {
	currentID := rf.getMe()
	currentTerm := rf.getTerm()

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.CandidateID = currentID
	args.Term = currentTerm
	
	rf.sendRequestVote(peerIndex, &args, &reply)

	if reply.Term > currentTerm {
		rf.convertTo(STATE_FOLLOWER) // Convert to follower
	} else {
		if reply.VoteGranted {
			electionBucket <- 1
		} else {
			electionBucket <- 0
		}
	}
}

func (rf* Raft) startCampaign(electionResult chan bool) {
	electionBucket := make(chan int, len(rf.peers))

	me := rf.getMe()
	peers := rf.getPeers()

	for peerIndex, _ := range peers {
		if peerIndex != me {
			go rf.askForVote(peerIndex, electionBucket)
		}
	}

	votesReceived := 1;
	rf.setVotedFor(me)

	for i := 0; i < len(peers) - 1; i++ {
		votesReceived += <- electionBucket 
		if votesReceived >= int(math.Ceil(float64(len(peers))) / 2.0) {
			electionResult <- true
			return
		}
	}

	electionResult <- false
}

func (rf* Raft) requestMajority() chan bool {	
	electionResult := make(chan bool, 1)
	
	go rf.startCampaign(electionResult)

	return electionResult
}

func (rf* Raft) liveCandidate() {
	resetTimer := true
	electionTimeout := MakeTimer(rf.generateTimeoutDuration(600, 800))
	electionsFinished := rf.requestMajority()
	rf.incrementTerm()

	for rf.getState() == STATE_CANDIDATE {
		select {
		case _, ok := <- electionTimeout:
			if !ok {
				log.Fatalf("Raft %d: Invalid timer candidate", rf.getMe())
			}
		case electionResult := <- electionsFinished:
			if electionResult {
				rf.convertTo(STATE_LEADER)
			}

		case rpc := <- rf.commandRequestChannel:
			rf.processCommand(rpc)
			
		}

		if resetTimer {
			electionTimeout = MakeTimer(rf.generateTimeoutDuration(600, 800))
			electionsFinished = rf.requestMajority()
			rf.incrementTerm()
		}
	}
}

func (rf* Raft) remindAuthorityTo(me, term, peer int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	args.Term = term
	args.LeaderID = me

	rf.sendHeartbeat(peer, &args, &reply)

	if reply.Term > term {
		rf.convertTo(STATE_FOLLOWER)
	}

}

func (rf *Raft) renewAuthority() {
	me := rf.getMe()
	term := rf.getTerm()
	peers := rf.getPeers()

	for i, _ := range peers {
		if i != me {
			go rf.remindAuthorityTo(me, term, i)
		}
	}
}

func (rf *Raft) liveLeader() {
	heartbeatTimeout := MakeTimer(rf.generateTimeoutDuration(600, 601))
	rf.renewAuthority()

	for rf.getState() == STATE_LEADER {
		
		select {
		case _, ok := <- heartbeatTimeout:
			if !ok {
				log.Fatalf("Raft %d: Invalid timer leader", rf.getMe())
			}
			rf.renewAuthority()
			heartbeatTimeout = MakeTimer(rf.generateTimeoutDuration(600, 601))
		case rpc := <- rf.commandRequestChannel:
			rf.processCommand(rpc)
		}
	}

}

func (rf *Raft) live() {

	for {
		state := rf.getState()

		switch state {
		case STATE_FOLLOWER: 
			rf.liveFollower()
		case STATE_CANDIDATE:
			rf.liveCandidate()
		case STATE_LEADER:
			rf.liveLeader()
		default:
			log.Fatalf("Raft %d: Unknown state - %d. Aborting...\n", rf.getMe(), state)
		}

	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.commandRequestChannel = make(chan commandProcessPipe, 5) // Arbitrarily chosen channel size
	rf.currentTerm = 1
	rf.currentState = STATE_FOLLOWER
	rf.votedFor = -1;

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.live()

	return rf
}
