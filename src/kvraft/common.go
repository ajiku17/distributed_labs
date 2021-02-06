package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClerkID   int64
	SeqNumber int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClerkID   int64
	SeqNumber int64
}

type GetReply struct {
	Err   Err
	Value string
}
