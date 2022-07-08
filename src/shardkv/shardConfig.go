package shardkv

import (
	"6.824/shardctrler"
)

// detect new configuration when all shard is serving
// which means current configuration has applied to all servers
func (kv *ShardKV) detectConfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.checkShardServing() {
		newConfig := kv.sm.Query(kv.config.Num + 1)
		if newConfig.Num == kv.config.Num+1 {
			kv.agreeCommand(Command{
				Type: Configuration,
				Data: newConfig,
			})
		}
	}
}

// check every shard is serving
func (kv *ShardKV) checkShardServing() bool {
	for _, v := range kv.kvDB {
		if v.Status != Serve {
			return false
		}
	}
	return true
}

// update new config
func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
	if newConfig.Num != kv.config.Num+1 {
		return
	}
	for shardId, groupId := range newConfig.Shards {
		if kv.config.Num == 0 {
			kv.kvDB[shardId].Status = Serve
			continue
		} else if groupId == kv.groupId && kv.config.Shards[shardId] != kv.groupId {
			// shard which not provide in current config should pull data from other group
			kv.kvDB[shardId].Status = Pull
		} else if groupId != kv.groupId && kv.config.Shards[shardId] == kv.groupId {
			// push the data of shard which not provide in new config
			kv.kvDB[shardId].Status = Push
		}
	}
	// DPrintf("[server-%d-%d] [Change] [Config-%d] [Upadte] new config = %v", kv.groupId, kv.me, kv.config.Num, kv.config)
	kv.config = newConfig
}
