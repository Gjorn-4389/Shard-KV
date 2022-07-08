package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	// Your data here.
	stateMachine ConfigStateMachine

	lastApplied int                   // prevent DB rollback
	waitCh      map[int]chan RPCReply // operation -> start()'s new index -> chan(Op)
	lastOp      map[int64]int64       // clientId -> SerialNo
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(RPCArgs{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.stateMachine = NewConfigArray()
	sc.waitCh = make(map[int]chan RPCReply)
	sc.lastOp = make(map[int64]int64)

	go sc.assignApplyMessage()

	return sc
}

// get Wait Channel, create if not exists
func (sc *ShardCtrler) getOrCreatWaitCh(idx int) chan RPCReply {
	if _, ok := sc.waitCh[idx]; !ok {
		sc.waitCh[idx] = make(chan RPCReply, 1)
	}
	return sc.waitCh[idx]
}

// close and delete waitCh after command execute
func (sc *ShardCtrler) deleteApplyCh(idx int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if ch, ok := sc.waitCh[idx]; ok {
		close(ch)
		delete(sc.waitCh, idx)
	}
}

func (sc *ShardCtrler) Operation(args *RPCArgs, reply *RPCReply) {
	// make command for send to raft server

	sc.mu.Lock()
	// check command is replicated
	if sc.isDuplicateOperation(*args) {
		DPrintf(Lab4A, "[ShardCtrler-%d] receive replicated operation", sc.me)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	idx, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		sc.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	// waiting command-idx reply
	ch := sc.getOrCreatWaitCh(idx)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(AgreementTimeout * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	// release wait channel
	go sc.deleteApplyCh(idx)
}

func (sc *ShardCtrler) assignApplyMessage() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.mu.Lock()

			// get stale command
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = msg.CommandIndex

			cmd, ok := msg.Command.(RPCArgs)
			// unable to convert the command to Op
			if !ok {
				sc.mu.Unlock()
				continue
			}

			result := RPCReply{}
			sc.executeCommand(cmd, &result)

			// if kvserver is leader, should send result to client
			// commited log Term != rf.currenterm  =====>  leader changes and server become leader again
			if ch, waitChExist := sc.waitCh[msg.CommandIndex]; waitChExist {
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch <- result
				}
			}

			sc.mu.Unlock()

		}
	}
}

func (sc *ShardCtrler) executeCommand(operation RPCArgs, reply *RPCReply) {
	// check whether duplicate command
	if sc.isDuplicateOperation(operation) {
		reply.Err = OK
		return
	}
	// execute command according to its type
	switch operation.OpType {
	case OpJoin:
		reply.Err = sc.stateMachine.Join(operation.Servers)
	case OpLeave:
		reply.Err = sc.stateMachine.Leave(operation.GIDs)
	case OpMove:
		reply.Err = sc.stateMachine.Move(operation.Shard, operation.GID)
	case OpQuery:
		reply.Config, reply.Err = sc.stateMachine.Query(operation.Num)
	}

	newConfig, _ := sc.stateMachine.Query(-1)
	DPrintf(Lab4A, "[ShardCtrler-%d] execute the command = %v, new Config = {Num=%v, Shards=%v, Groups=%v}", sc.me, operation, newConfig.Num, newConfig.Shards, newConfig.Groups)

	sc.saveResult(operation)
}

// check whether command is duplicated by lastOperation[clientId] >= command index
func (sc *ShardCtrler) isDuplicateOperation(operation RPCArgs) bool {
	if operation.OpType == OpQuery {
		return false
	}
	return sc.lastOp[operation.ClerkId] >= operation.SeqNo
}

// save result for join, leave or move command
func (sc *ShardCtrler) saveResult(operation RPCArgs) {
	if operation.OpType != OpQuery && operation.SeqNo > sc.lastOp[operation.ClerkId] {
		sc.lastOp[operation.ClerkId] = operation.SeqNo
	}
}
