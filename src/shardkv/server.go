package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	sm       *shardctrler.Clerk
	make_end func(string) *labrpc.ClientEnd
	groupId  int
	ctrlers  []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// config
	config      shardctrler.Config
	lastApplied int

	kvDB   map[int]*Shard        // shardId -> shard
	waitCh map[int]chan RPCReply // commandId -> chan
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// do action loop, action should lock() and unlock() by self
func (kv *ShardKV) daemon(do func()) {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			do()
		}
		time.Sleep(ActionTimeout * time.Millisecond)
	}
}

// send commond to raft in order to get agreement with followers
// return command index:
// 		return -1 when this server not leader
func (kv *ShardKV) agreeCommand(cmd Command) int {
	// raft will confirm that concurrent consistent, no need for lock kvserver
	idx, _, _ := kv.rf.Start(cmd)
	if idx > 0 {
		DPrintf("[server-%d-%d] [AgreeCommand] idx = %v,  cmd = %v", kv.groupId, kv.me, idx, cmd)
	}
	return idx
}

// copy kvdb for snapshot
func (kv *ShardKV) kvDBCopy() map[int]Shard {
	copy := make(map[int]Shard)
	for shardId, shard := range kv.kvDB {
		copy[shardId] = shard.deepCopy()
	}
	return copy
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(Shard{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(OperationArgs{})
	labgob.Register(RPCReply{})
	labgob.Register(ShardDataArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.groupId = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// all shard are serving
	kv.kvDB = NewKVDB()
	kv.waitCh = make(map[int]chan RPCReply)
	DPrintf("[server-%d-%d] [Change] [Config-%d] [start]", kv.groupId, kv.me, kv.config.Num)
	kv.config = kv.sm.Query(0)
	kv.readSnapshot(persister.ReadSnapshot())

	// assign message from raft
	go kv.assignApplyMessage()

	// pull new config 10/s when all shards normal
	go kv.daemon(kv.detectConfig)

	// check any shard should push data
	go kv.daemon(kv.checkPushShardData)

	// add new entry at new term
	go kv.daemon(kv.checkEntryInCurrentTerm)

	// DPrintf("[server-%d-%d] [Config-%d] [Start]", kv.groupId, kv.me, kv.config.Num)

	return kv
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	return kv.maxraftstate-kv.persister.RaftStateSize() < kv.maxraftstate/10
}

func (kv *ShardKV) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDBCopy())
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvDB = make(map[int]Shard)
	var cfg shardctrler.Config

	if d.Decode(&kvDB) != nil || d.Decode(&cfg) != nil {
		DPrintf("[server-%d-%d] [readSnapshot] Wrong!", kv.groupId, kv.me)
		return
	}

	kv.config = cfg
	for shardId, shard := range kvDB {
		copy := shard.deepCopy()
		kv.kvDB[shardId] = &copy
	}

	DPrintf("[server-%d-%d] [readSnapshot] [Config-%d] [lastApplied-%d] current map = %v", kv.groupId, kv.me, kv.config.Num, kv.lastApplied, kv.kvDBCopy())
}

// get Wait Channel, create if not exists
func (kv *ShardKV) getOrCreateWaitCh(idx int) chan RPCReply {
	if _, ok := kv.waitCh[idx]; !ok {
		kv.waitCh[idx] = make(chan RPCReply, 1)
	}
	return kv.waitCh[idx]
}

// close and delete waitCh after command execute
func (kv *ShardKV) closeApplyCh(idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ch, ok := kv.waitCh[idx]; ok {
		close(ch)
		delete(kv.waitCh, idx)
	}
}

// KV Operation RPC handler
func (kv *ShardKV) Operation(args *OperationArgs, reply *RPCReply) {
	// defer func() {
	// 	kv.mu.Lock()
	// 	DPrintf("[server-%d-%d] [config-%d] [lastApplied-%d] [Operation] args = %v, reply = %v", kv.groupId, kv.me, kv.config.Num, kv.lastApplied, args, reply)
	// 	kv.mu.Unlock()
	// }()

	kv.mu.Lock()
	// check command is replicated
	if kv.isDuplicateOperation(*args) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	// kv don't serve for shard in this configuration || when updating config
	shardId := key2shard(args.Key)
	if kv.config.Shards[shardId] != kv.groupId || kv.kvDB[shardId].Status != Serve {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// check server is leader && send command to raft
	idx := kv.agreeCommand(Command{
		Type: Operation,
		Data: *args,
	})
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
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(OperationTimeout * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	// release wait channel asynchronize
	kv.closeApplyCh(idx)
}

// assign apply message from raft to handler
func (kv *ShardKV) assignApplyMessage() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			// get stale command
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			DPrintf("[server-%d-%d] [Config-%d] [assignApplyMessage] %v", kv.groupId, kv.me, kv.config.Num, msg)

			// execute command
			result := RPCReply{}
			cmd := msg.Command.(Command)
			kv.executeCommand(cmd, &result)

			// server is leader and it is in the command Term, it should send result to client
			// commited log Term != rf.currenterm  =====>  leader changes and server become leader again
			if ch, waitChExist := kv.waitCh[msg.CommandIndex]; waitChExist {
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch <- result
				}
			}

			// need snapshot
			if kv.needSnapshot() {
				// DPrintf("[server-%d-%d] [Config-%d] [assignApplyMessage] [MakeSnapshot] idx = %v", kv.groupId, kv.me, kv.config.Num, kv.lastApplied)
				kv.rf.Snapshot(kv.lastApplied, kv.snapshotData())
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// install snapshot
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readSnapshot(msg.Snapshot)
				// DPrintf("[server-%d-%d] [Config-%d] [assignSnapshotMessage] [readSnapshot] lastApplied = %v", kv.groupId, kv.me, kv.config.Num, msg.SnapshotIndex)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", msg))
		}
	}
}

// execute command from raft according to its command type
func (kv *ShardKV) executeCommand(cmd Command, reply *RPCReply) {
	switch cmd.Type {
	case Operation:
		kv.executeKVOperation(cmd.Data.(OperationArgs), reply)
	case Configuration:
		kv.updateConfig(cmd.Data.(shardctrler.Config))
	case InsertShards:
		kv.insertShard(cmd.Data.(ShardDataArgs), reply)
	case DeleteShards:
		kv.deleteShard(cmd.Data.(ShardDataArgs))
	case EmptyEntry:
		DPrintf("[server-%d-%d] [EmptyEntry]", kv.groupId, kv.me)
	}
}

func (kv *ShardKV) checkEntryInCurrentTerm() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.agreeCommand(Command{
			Type: EmptyEntry,
		})
	}
}
