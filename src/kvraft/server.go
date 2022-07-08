package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	lastApplied int
	kvDB        KVStateMachine
	lastOp      map[int64]int64
	waitCh      map[int]chan OperationReply
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OperationArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvDB = NewMemoryKV()
	kv.waitCh = make(map[int]chan OperationReply)
	kv.lastOp = make(map[int64]int64)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.assignApplyMessage()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	DPrintf("[kvserver-%d] [needSnapshot] [RaftSize] %d", kv.me, kv.persister.RaftStateSize())
	return kv.maxraftstate-kv.persister.RaftStateSize() < kv.maxraftstate/10
}

func (kv *KVServer) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastOp)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvDB = NewMemoryKV()
	var opResult map[int64]int64

	if d.Decode(&kvDB) != nil || d.Decode(&opResult) != nil {
		DPrintf("[kvserver-%d] [readSnapshot] read Wrong!", kv.me)
		return
	}

	kv.kvDB, kv.lastOp = kvDB, opResult
	DPrintf("[KVServer-%d] [lastApplied-%v] [readSnapshot] logs = %v, current kv map = %v", kv.me, kv.lastApplied, kv.rf.GetLogs(), kv.kvDB.Print())
}

// get Wait Channel, create if not exists
func (kv *KVServer) getOrCreateWaitCh(idx int) chan OperationReply {
	if _, ok := kv.waitCh[idx]; !ok {
		kv.waitCh[idx] = make(chan OperationReply, 1)
	}
	return kv.waitCh[idx]
}

// close and delete waitCh after command execute
func (kv *KVServer) deleteApplyCh(idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ch, ok := kv.waitCh[idx]; ok {
		close(ch)
		delete(kv.waitCh, idx)
	}
}

func (kv *KVServer) Operation(args *OperationArgs, reply *OperationReply) {
	defer DPrintf("[KVServer-%d] [Operation] args = %v, reply = %v", kv.me, args, reply)

	kv.mu.Lock()
	// check command is replicated
	if kv.isDuplicateOperation(*args) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	idx, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getOrCreateWaitCh(idx)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(AgreementTimeout * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	kv.deleteApplyCh(idx)
}

func (kv *KVServer) assignApplyMessage() {
	for !kv.killed() {
		// receive message from raft
		msg := <-kv.applyCh
		if msg.CommandValid {
			DPrintf("[KVServer-%d] [ApplyMessage] [Command] message = %v", kv.me, msg)
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				DPrintf("[KVServer-%d] [ApplyMessage] [Outdate] message = %v, lastApplied = %d", kv.me, msg, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			// execute operation
			var reply OperationReply
			cmd := msg.Command.(OperationArgs)
			kv.executeOperation(cmd, &reply)

			// if kvserver is leader, should send result to client
			// commited log Term != rf.currenterm  =====>  leader changes and server become leader again
			if ch, waitChExist := kv.waitCh[msg.CommandIndex]; waitChExist {
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					// consume message in channel to avoid block
					select {
					case <-ch:
					default:
					}
					ch <- reply
				}
			}

			if kv.needSnapshot() {
				kv.rf.Snapshot(kv.lastApplied, kv.snapshotData())
				DPrintf("[KVServer-%d] [ApplyMessage] [MakeSnapshot] lastApplied = %d, currentLog = %v", kv.me, kv.lastApplied, kv.rf.GetLogs())
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			DPrintf("[KVServer-%d] [ApplyMessage] [Snapshot] messageIndex = %v, logs = %v", kv.me, msg.SnapshotIndex, kv.rf.GetLogs())
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", msg))
		}
	}
}

// only execute put or append
func (kv *KVServer) executeOperation(operation OperationArgs, reply *OperationReply) {
	// check whether duplicate command, only put or append
	if kv.isDuplicateOperation(operation) {
		// DPrintf("[KVServer-%d] [executeOperation] [Duplicate] maxAppliedCommandId = %v, clientId = %v", kv.me, kv.lastOp[operation.ClientId], operation.ClientId)
		reply.Err = OK
		return
	}

	// execute command according to its type
	switch operation.OpType {
	case OpGet:
		reply.Value, reply.Err = kv.kvDB.Get(operation.Key)
	case OpPut:
		reply.Err = kv.kvDB.Put(operation.Key, operation.Value)
	case OpAppend:
		reply.Err = kv.kvDB.Append(operation.Key, operation.Value)
	}

	// only Put/Append will change kvdb & save command
	// isDuplicateOperation -> operation.SeqNo > kv.lastOp[operation.ClientId]
	if operation.OpType != OpGet {
		kv.lastOp[operation.ClientId] = operation.SeqNo
	}
}

// check whether command is duplicated by lastOperation[clientId] >= command index
func (kv *KVServer) isDuplicateOperation(args OperationArgs) bool {
	if args.OpType == OpGet {
		return false
	}
	return kv.lastOp[args.ClientId] >= args.SeqNo
}
