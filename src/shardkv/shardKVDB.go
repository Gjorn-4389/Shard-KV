package shardkv

import "6.824/shardctrler"

type ShardStatus string

type Shard struct {
	Status ShardStatus
	KV     map[string]string
	LastOp map[int64]int64 // last operation serial number
}

func NewKVDB() map[int]*Shard {
	kvdb := make(map[int]*Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		kvdb[i] = &Shard{
			Status: Serve,
			KV:     make(map[string]string),
			LastOp: make(map[int64]int64),
		}
	}
	return kvdb
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	// DPrintf("[Shard] [Put] key = %v, value = %v, res = %v", key, value, shard.KV[key])
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	// DPrintf("[Shard] [Append] key = %v, value = %v, res = %v", key, value, shard.KV[key])
	return OK
}

func (shard *Shard) deepCopy() Shard {
	newShard := Shard{Status: shard.Status}
	newShard.KV = make(map[string]string)
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	newShard.LastOp = make(map[int64]int64)
	for k, v := range shard.LastOp {
		newShard.LastOp[k] = v
	}
	return newShard
}

// check whether command is duplicated by lastOperation[clientId] >= command index
func (kv *ShardKV) isDuplicateOperation(operation OperationArgs) bool {
	if operation.OpType == OpGet {
		return false
	}
	if lastSeqNo, ok := kv.kvDB[key2shard(operation.Key)].LastOp[operation.ClientId]; ok {
		return lastSeqNo >= operation.SeqNo
	}
	return false
}

// execute command from raft
func (kv *ShardKV) executeKVOperation(operation OperationArgs, reply *RPCReply) {
	// check whether duplicate command
	if kv.isDuplicateOperation(operation) {
		// DPrintf("[server-%d-%d] [Shard-%d] [executeKVOperation] [Duplicate] operation = %v", kv.groupId, kv.me, operation)
		reply.Err = OK
		return
	}

	// check shard is able to do operation
	shardId := key2shard(operation.Key)
	if kv.kvDB[shardId].Status != Serve {
		// DPrintf("[server-%d-%d] [Shard-%d] [executeKVOperation] [Wrong] operation = %v, status = %v", kv.groupId, kv.me, shardId, operation, kv.kvDB[shardId].Status)
		reply.Err = ErrWrongGroup
		return
	}

	// execute command according to its type
	switch operation.OpType {
	case OpGet:
		reply.Value, reply.Err = kv.kvDB[shardId].Get(operation.Key)
	case OpPut:
		reply.Err = kv.kvDB[shardId].Put(operation.Key, operation.Value)
	case OpAppend:
		reply.Err = kv.kvDB[shardId].Append(operation.Key, operation.Value)
	}

	if operation.OpType != OpGet {
		kv.kvDB[shardId].LastOp[operation.ClientId] = operation.SeqNo
	}
}
