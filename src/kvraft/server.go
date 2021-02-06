package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET_OP    = 1
	PUT_OP    = 2
	APPEND_OP = 3
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	Op	   interface{}
}


type GetOp struct {
	Key       string
	ClerkID   int64
	SeqNumber int64
}

type PutAppendOp struct {
	Key       string
	Value     string
	ClerkID   int64
	SeqNumber int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data       map[string]string
	appliedOps map[int]interface {}
	clerkTable map[int64]int64
}


func (kv *KVServer) waitForApply(index int, term int, Op Op) bool {

	for {
		DPrintf("KVServer %d yee haaaaw", kv.me)
		kv.mu.Lock()
		DPrintf("KVServer %d in waiting loop for op %v at index %d term %d", kv.me, Op, index, term)

		if kv.killed() {
			DPrintf("KVServer %d has been killed, stopping wait for apply", kv.me)
			kv.mu.Unlock()
			return false
		}

		op, ok := kv.appliedOps[index]
		if ok {
			DPrintf("KVServer %d has applied op %v at index %d; waiting loop for op %v at index %d term %d", kv.me, op, index, Op, index, term)
			if op == Op {
				DPrintf("KVServer wait loop returning true")
				delete(kv.appliedOps, index)
				kv.mu.Unlock()
				return true
			}
			DPrintf("KVServer wait loop returning false")
			kv.mu.Unlock()
			return false
		} else {
			DPrintf("KVServer %d does not have anything applied; waiting loop for op %v at index %d term %d", kv.me, Op, index, term)
		}

		DPrintf("KVServer %d wait loop calling getState", kv.me)
		rfTerm, rfLeader := kv.rf.GetState()
		DPrintf("KVServer %d wait loop raft GetState returned with term %d isLeader %v", kv.me, term, rfLeader)


		if rfTerm != term || !rfLeader {
			DPrintf("KVServer %d is no longer leader; in waiting loop for op %v at index %d term %d", kv.me, Op, index, term)
			kv.mu.Unlock()
			return false
		}
		
		DPrintf("KVServer %d sleeping in waiting loop for op %v at index %d term %d", kv.me, Op, index, term)
		kv.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	DPrintf("KVServer %d received a Get request %v current data state: %v", kv.me, args, kv.data)
	wrapper := Op{}
	wrapper.OpType = GET_OP
	wrapper.Op = GetOp { Key       : args.Key,
						 ClerkID   : args.ClerkID,
						 SeqNumber : args.SeqNumber, }

	index, term, isLeader := kv.rf.Start(wrapper);
	if !isLeader {
		DPrintf("KVServer %d is not a leader, returning ErrWrongLeader", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf("KVServer %d is leader, op index: %d op term: %d", kv.me, index, term)
	kv.mu.Unlock()

	DPrintf("KVServer %d waiting for reply on Get op, op index: %d op term: %d", kv.me, index, term)
	opApplied := kv.waitForApply(index, term, wrapper)
	DPrintf("KVServer %d recieved a reply: %v on Get op, op index: %d op term: %d", kv.me, opApplied, index, term)
	if !opApplied {
		DPrintf("KVServer %d didn't apply Get op %v at index %d", kv.me, wrapper, index)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	DPrintf("KVServer %d getting key %s from data", kv.me, args.Key)
	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	DPrintf("KVServer %d got value %s on key %s", kv.me, reply.Value, args.Key)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	DPrintf("KVServer %d received a PutAppend request %v current data state: %v", kv.me, args, kv.data)
	wrapper := Op{}
	if args.Op == "Put" {
		wrapper.OpType = PUT_OP
	} else {
		wrapper.OpType = APPEND_OP
	}
	wrapper.Op = PutAppendOp { Key       : args.Key, 
							   Value     : args.Value,
							   ClerkID   : args.ClerkID,
							   SeqNumber : args.SeqNumber, }

	index, term, isLeader := kv.rf.Start(wrapper)
	if !isLeader {
		DPrintf("KVServer %d is not a leader, returning ErrWrongLeader", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf("KVServer %d is leader, op index: %d op term: %d", kv.me, index, term)
	kv.mu.Unlock()

	DPrintf("KVServer %d waiting for reply on PutAppend op, op index: %d op term: %d", kv.me, index, term)
	opApplied := kv.waitForApply(index, term, wrapper)
	DPrintf("KVServer %d recieved a reply: %v on PutAppend op, op index: %d op term: %d", kv.me, opApplied, index, term)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !opApplied {
		DPrintf("KVServer %d didn't apply PutAppend op %v at index %d", kv.me, wrapper, index)
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	DPrintf("KVServer %d finished PutAppend op, current Value: %s", kv.me, kv.data[args.Key])
}

func (kv *KVServer) placeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data       map[string]string
	var clerkTable map[int64]int64

	if d.Decode(&clerkTable) != nil ||
	   d.Decode(&data) != nil {
	  DPrintf("Error while decoding")
	} else {
	  kv.clerkTable = clerkTable
	  kv.data = data
	}
}

func (kv *KVServer) createSnapshot(raftLogIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.clerkTable)
	DPrintf("KVServer %d size of clerkTable %v", kv.me, len(w.Bytes()))
	e.Encode(kv.data)
	DPrintf("KVServer %d size of clerkTable + applied ops + data %v", kv.me, len(w.Bytes()))

	kv.rf.TakeSnapshot(raftLogIndex, w.Bytes())
}

func (kv *KVServer) applyWatcher() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			DPrintf("KVServer %d has been killed, stopping apply watcher", kv.me)
			kv.mu.Unlock()
			return 
		}
		kv.mu.Unlock()

		applyMsg := <- kv.applyCh
		DPrintf("KVServer %d received an apply message %v", kv.me, applyMsg)

		kv.mu.Lock()
		
		if applyMsg.Snapshot {
			kv.placeSnapshot(applyMsg.SnapshotData)
		} else {

			op := applyMsg.Command.(Op)

			if op.OpType != GET_OP {
				
				putAppendOp := op.Op.(PutAppendOp)
				if seqNumber, ok := kv.clerkTable[putAppendOp.ClerkID]; !ok || seqNumber != putAppendOp.SeqNumber { 
					if op.OpType == APPEND_OP {
						kv.data[putAppendOp.Key] += putAppendOp.Value
					} else if op.OpType == PUT_OP {
						kv.data[putAppendOp.Key] = putAppendOp.Value
					}
					kv.clerkTable[putAppendOp.ClerkID] = putAppendOp.SeqNumber
				} else {
					DPrintf("KVServer %d dropper a duplicate packet %v", kv.me, putAppendOp)
				}
			}
			
			DPrintf("KVServer %d updating appliedOps with new applyMsg %v", kv.me, applyMsg)
			if applyMsg.CommandValid {
				kv.appliedOps[applyMsg.CommandIndex] = op
			}

			DPrintf("KVServer %d Successfully applied op %v", kv.me, op)

			if kv.maxraftstate != -1  && kv.rf.GetRaftStateSize() - 400 > kv.maxraftstate {
				kv.createSnapshot(applyMsg.CommandIndex)
			} 
		}

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetOp{})
	labgob.Register(PutAppendOp{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.appliedOps = make(map[int]interface{})
	kv.clerkTable = make(map[int64]int64)

	go kv.applyWatcher()

	return kv
}
