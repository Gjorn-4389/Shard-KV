package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leaderId int
	me       int64
	seqNo    int64
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
	ck.me = nrand()
	ck.leaderId, ck.seqNo = 0, 1
	return ck
}

func (ck *Clerk) operation(key string, value string, op string) string {
	// You will have to modify this function.
	args := OperationArgs{Key: key, Value: value, OpType: op, SeqNo: ck.seqNo, ClientId: ck.me}
	ck.seqNo++

	for {
		var reply OperationReply
		DPrintf("[Clerk-%d] [Operation] [Request] args = %v, reply = %v", ck.me, args, reply)
		ok := ck.servers[ck.leaderId].Call("KVServer.Operation", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("[Clerk-%d] [Operation] [Reply] args = %v, reply = %v", ck.me, args, reply)
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.operation(key, "", OpGet)
}

func (ck *Clerk) Put(key string, value string) {
	ck.operation(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.operation(key, value, OpAppend)
}
