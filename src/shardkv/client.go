package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	clientId  int64
	seqNo     int64
	leaderIds map[int]int // groupId -> leaderId
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderIds = make(map[int]int)
	ck.clientId = nrand()
	ck.seqNo = 1
	ck.config = ck.sm.Query(-1)

	return ck
}

func (ck *Clerk) operation(key string, value string, op string) string {
	args := OperationArgs{Key: key, Value: value, OpType: op, SeqNo: ck.seqNo, ClientId: ck.clientId}
	ck.seqNo++

	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for i := 0; i < len(servers); i++ {
				serverIdx := (i + ck.leaderIds[gid]) % len(servers)
				var reply RPCReply
				// DPrintf("[Clerk-%d]->[group-%d] [Config-%d] send args = %v ", ck.clientId, gid, ck.config.Num, args)
				ok := ck.make_end(servers[serverIdx]).Call("ShardKV.Operation", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.leaderIds[gid] = serverIdx
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					// change group
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
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
