package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shardId -> gid
	Groups map[int][]string // gid -> servers[] (serverName[])
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrorWrongLeader"

	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"

	RPCRetryInterval = 100
	AgreementTimeout = 500
)

type Err string

type RPCArgs struct {
	OpType  string
	ClerkId int64
	SeqNo   int64

	// Join RPC
	Servers map[int][]string // new GID -> servers mappings
	// Leave RPC
	GIDs []int
	// Move RPC
	Shard int
	GID   int
	// Query RPC
	Num int // desired config number
}

type RPCReply struct {
	Err string

	// query reply
	Config Config
}
