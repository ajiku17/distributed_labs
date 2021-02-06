package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int
	id         int64
	seq        int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.prevLeader = 0
	ck.id = nrand()
	ck.seq = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args  := GetArgs{ Key       : key,
					  ClerkID   : ck.id,
					  SeqNumber : ck.seq}
	reply := GetReply{}

	ck.seq++

	serverInd := ck.prevLeader
	for {
		DPrintf("Clerk calling Get %v to KVServer %d", args, serverInd)
		ok := ck.servers[serverInd].Call("KVServer.Get", &args, &reply)
		if !ok {
			serverInd = (serverInd + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.prevLeader = serverInd;
			return reply.Value
		case ErrNoKey:
			ck.prevLeader = serverInd;
			return ""
		case ErrWrongLeader:
			serverInd = (serverInd + 1) % len(ck.servers)
		default:
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args  := PutAppendArgs{ Key       : key,
							Value     : value,
							Op        : op,
						    ClerkID   : ck.id,
					        SeqNumber : ck.seq}
	reply := PutAppendReply{}

	ck.seq++

	serverInd := ck.prevLeader
	for {
		DPrintf("Clerk calling PutAppend %v to KVServer %d", args, serverInd)
		ok := ck.servers[serverInd].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			serverInd = (serverInd + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.prevLeader = serverInd;
			return
		case ErrWrongLeader:
			serverInd = (serverInd + 1) % len(ck.servers)
		default:
			DPrintf("Clerk in PutAppend: Unknown error message %v", reply.Err)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
