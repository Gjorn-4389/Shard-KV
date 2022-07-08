package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrConfigNo    = "ErrConfigError"

	// operation type
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"

	// command type
	Operation     = "Operation"
	Configuration = "Configuration"
	InsertShards  = "InsertShards"
	DeleteShards  = "DeleteShards"
	EmptyEntry    = "EmptyEntry"

	// shard status
	Serve ShardStatus = "Serving" // provide read/write service
	Pull  ShardStatus = "Pull"    // get data from another group
	Push  ShardStatus = "Push"    // send data to another group

	// shard data type
	PullShardData = "PullShardData"
	PushShardData = "PushShardData"

	// server timeout
	OperationTimeout = 500
	ActionTimeout    = 100
)

type CommandType string
type Err string

// command structure which send to raft
type Command struct {
	Type CommandType

	// Operation: OperationArgs
	// Configuration: Config
	// InsertShards: Shard
	// DeleteShards: shardId
	Data interface{}
}

// just for
type KVOp struct {
	ClientId int64
	SeqNo    int64
	OpType   string
	Key      string
	Value    string
}

// Between groups and client
type OperationArgs struct {
	SeqNo    int64
	ClientId int64
	OpType   string // "Put" or "Append" or "Get" or "PullShardData"
	Key      string
	Value    string
}

type RPCReply struct {
	Err      Err
	Value    string
	ConfigNo int
}

// Between groups
type ShardDataArgs struct {
	ConfigNo  int
	ShardId   int
	ShardData Shard
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
