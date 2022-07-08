package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId int64
	seqNo   int64
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
	// Your code here.
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) nextCommandIndex() int64 {
	ck.seqNo++
	return ck.seqNo
}

func (ck *Clerk) operation(args *RPCArgs) Config {
	args.ClerkId = ck.clerkId
	args.SeqNo = ck.nextCommandIndex()
	DPrintf(Lab4A, "[client-%d] want to do operation, args={clientId=%v, serialNo=%v, OpType=%s, Join{Servers=%v}, Leave{GIDs=%v}, Move{Shard=%d, GID=%d}, Query{Num=%d}}", ck.clerkId, args.ClerkId, args.SeqNo, args.OpType, args.Servers, args.GIDs, args.Shard, args.GID, args.Num)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply RPCReply
			if ok := srv.Call("ShardCtrler.Operation", args, &reply); ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(RPCRetryInterval * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &RPCArgs{OpType: OpQuery, Num: num}
	return ck.operation(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &RPCArgs{OpType: OpJoin, Servers: servers}
	ck.operation(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &RPCArgs{OpType: OpLeave, GIDs: gids}
	ck.operation(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &RPCArgs{OpType: OpMove, Shard: shard, GID: gid}
	ck.operation(args)
}
