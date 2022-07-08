package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"

	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"

	AgreementTimeout = 500
)

type Err string

type OperationArgs struct {
	SeqNo    int64
	ClientId int64
	OpType   string // "Put" or "Append" or "Get"

	Key   string
	Value string
}

type OperationReply struct {
	Err   Err
	Value string
}

type LastOperationReply struct {
	SeqNo     int64
	LastReply OperationReply
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
