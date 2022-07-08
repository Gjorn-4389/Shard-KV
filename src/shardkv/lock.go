package shardkv

import (
	"runtime"

	"6.824/labrpc"
)

// func (kv *ShardKV) Unlock() {
// 	DPrintf("[server-%d-%v] [unlock] caller = %v", kv.groupId, kv.me, MyCaller())
// 	kv.mu.Unlock()
// }

// func (kv *ShardKV) Lock(tag ...string) {
// 	tg := ""
// 	if len(tag) > 0 {
// 		tg = tag[0]
// 	}
// 	DPrintf("[server-%d-%v] [wantlock] caller = %v, tg = %v", kv.groupId, kv.me, MyCaller(), tg)
// 	kv.mu.Lock()
// 	DPrintf("[server-%d-%v] [lock] caller = %v, tg = %v", kv.groupId, kv.me, MyCaller(), tg)
// }

func (ck *Clerk) Call(e *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	DPrintf("[Clerk-%d] [args] call method %v, args = %v", ck.clientId, svcMeth, args)
	ok := e.Call(svcMeth, args, reply)
	DPrintf("[Clerk-%d] [reply] call method %v, args = %v, reply = %v", ck.clientId, svcMeth, args, reply)
	return ok
}

func getFrame(skipFrames int) runtime.Frame {
	// We need the frame at index skipFrames+2, since we never want runtime.Callers and getFrame
	targetFrameIndex := skipFrames + 2

	// Set size to targetFrameIndex+2 to ensure we have room for one more caller than we need
	programCounters := make([]uintptr, targetFrameIndex+2)
	n := runtime.Callers(0, programCounters)

	frame := runtime.Frame{Function: "unknown"}
	if n > 0 {
		frames := runtime.CallersFrames(programCounters[:n])
		for more, frameIndex := true, 0; more && frameIndex <= targetFrameIndex; frameIndex++ {
			var frameCandidate runtime.Frame
			frameCandidate, more = frames.Next()
			if frameIndex == targetFrameIndex {
				frame = frameCandidate
			}
		}
	}

	return frame
}

// MyCaller returns the caller of the function that called it :)
func MyCaller() string {
	// Skip GetCallerFunctionName and the function to get the caller of
	return getFrame(2).Function
}
