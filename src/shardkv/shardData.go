package shardkv

import (
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// send shard data to need group's leader
func (kv *ShardKV) checkPushShardData() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for i := 0; i < shardctrler.NShards; i++ {
		if kv.kvDB[i].Status == Push {
			// DPrintf("[server-%d-%d] [Config-%d] [Shard-%d] status = %v ", kv.groupId, kv.me, kv.config.Num, i, kv.kvDB[i].Status)
			groupId := kv.config.Shards[i]
			for _, serverName := range kv.config.Groups[groupId] {
				server := kv.make_end(serverName)
				go kv.pushShardData(i, server)
			}
		}
	}
}

func (kv *ShardKV) pushShardData(shardId int, server *labrpc.ClientEnd) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	// check status
	if kv.kvDB[shardId].Status != Push {
		kv.mu.Unlock()
		return
	}
	args := ShardDataArgs{
		ConfigNo:  kv.config.Num,
		ShardId:   shardId,
		ShardData: kv.kvDB[shardId].deepCopy(),
	}
	kv.mu.Unlock()

	// send shardDataRPC
	var reply RPCReply
	if ok := server.Call("ShardKV.ShardData", &args, &reply); ok {
		kv.mu.Lock()
		if args.ConfigNo != kv.config.Num {
			kv.mu.Unlock()
			return
		}

		if reply.Err == OK || reply.ConfigNo > kv.config.Num {
			// receive OK || target server' configNo > local config no
			//          ==> delete self shardData
			kv.agreeCommand(Command{
				Type: DeleteShards,
				Data: ShardDataArgs{
					ConfigNo: args.ConfigNo,
					ShardId:  shardId,
				},
			})
		}
		kv.mu.Unlock()
	}

}

// handle PullShardData RPC
func (kv *ShardKV) ShardData(args *ShardDataArgs, reply *RPCReply) {
	defer DPrintf("[server-%d-%d] [Shard-%d] [ShardDataRPC] [Pull] args={ConfigNo=%v, ShardId=%v, ShardData=%v}, reply=%v", kv.groupId, kv.me, args.ShardId, args.ConfigNo, args.ShardId, args.ShardData, reply)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	reply.ConfigNo = kv.config.Num

	if args.ConfigNo != kv.config.Num {
		reply.Err = ErrConfigNo
		kv.mu.Unlock()
		return
	}

	if kv.kvDB[args.ShardId].Status != Pull {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	cmd := Command{
		Type: InsertShards,
		Data: *args,
	}

	// check server is leader && send command to raft
	idx := kv.agreeCommand(cmd)
	if idx == -1 {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	// waiting command-idx reply
	ch := kv.getOrCreateWaitCh(idx)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(OperationTimeout * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	// release wait channel asynchronize
	go kv.closeApplyCh(idx)
}

func (kv *ShardKV) insertShard(args ShardDataArgs, reply *RPCReply) {
	// insertShard command arrives ahead
	if args.ConfigNo > kv.config.Num {
		reply.Err = ErrConfigNo
		return
	}
	// has applied this command
	if args.ConfigNo < kv.config.Num || kv.kvDB[args.ShardId].Status != Pull {
		reply.Err = OK
		return
	}
	copy := args.ShardData.deepCopy()
	kv.kvDB[args.ShardId] = &copy
	kv.kvDB[args.ShardId].Status = Serve
	reply.Err = OK
	// DPrintf("[server-%d-%d] [Shard-%d] [Config-%d] [InsertShard] current db = {data=%v, status=%v, lastop=%v}, args = %v", kv.groupId, kv.me, args.ShardId, kv.config.Num, kv.kvDB[args.ShardId].KV, kv.kvDB[args.ShardId].Status, kv.kvDB[args.ShardId].LastOp, args)
}

func (kv *ShardKV) deleteShard(args ShardDataArgs) {
	// defer DPrintf("[server-%d-%d] [Shard-%d] [Config-%d] [DeleteShard] current db = {data=%v, status=%v, lastop=%v}, args = %v", kv.groupId, kv.me, args.ShardId, kv.config.Num, kv.kvDB[args.ShardId].KV, kv.kvDB[args.ShardId].Status, kv.kvDB[args.ShardId].LastOp, args)
	if args.ConfigNo != kv.config.Num || kv.kvDB[args.ShardId].Status != Push {
		return
	}
	kv.kvDB[args.ShardId].Status = Serve
	kv.kvDB[args.ShardId].KV = make(map[string]string)
	kv.kvDB[args.ShardId].LastOp = make(map[int64]int64)
}
