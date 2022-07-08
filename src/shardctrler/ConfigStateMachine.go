package shardctrler

import "sort"

type ConfigStateMachine interface {
	Join(map[int][]string) string
	Leave([]int) string
	Move(int, int) string
	Query(int) (Config, string)
}

const (
	freeGid int = 0
)

type ConfigArray struct {
	Configs []Config
}

func NewConfigArray() *ConfigArray {
	cf := ConfigArray{}
	cf.Configs = make([]Config, 1)
	for i := 0; i < NShards; i++ {
		cf.Configs[0].Shards[i] = freeGid
	}
	return &cf
}

func (ca *ConfigArray) Join(serverMap map[int][]string) string {
	newConfig := ca.createConfig()

	for gid, servers := range serverMap {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	shardCount := ca.groupByGid(newConfig)

	for {
		maxGid, minGid := ca.getMaxShardGid(shardCount), ca.getMinShardGid(shardCount)
		if maxGid != freeGid && len(shardCount[maxGid])-len(shardCount[minGid]) <= 1 {
			break
		}
		shardCount[minGid] = append(shardCount[minGid], shardCount[maxGid][0])
		shardCount[maxGid] = shardCount[maxGid][1:]
	}
	var newShards [NShards]int
	for gid, shards := range shardCount {
		for _, shardIdx := range shards {
			newShards[shardIdx] = gid
		}
	}
	newConfig.Shards = newShards
	ca.Configs = append(ca.Configs, newConfig)
	return OK
}

func (ca *ConfigArray) Leave(gids []int) string {
	newConfig := ca.createConfig()
	// gid -> shards
	shardCount := ca.groupByGid(newConfig)
	freeShards := []int{}
	for _, gid := range gids {
		// delete group
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// count free shards
		if shards, ok := shardCount[gid]; ok {
			freeShards = append(freeShards, shards...)
			delete(shardCount, gid)
		}
	}

	var newShards [NShards]int
	// assign free shards to minimum group
	if len(newConfig.Groups) != 0 {
		// assign free shard to group which has min number of shards
		for _, shardIdx := range freeShards {
			minGid := ca.getMinShardGid(shardCount)
			shardCount[minGid] = append(shardCount[minGid], shardIdx)
		}
		// construct map: shardIdx -> gid
		for gid, shards := range shardCount {
			for _, shardIdx := range shards {
				newShards[shardIdx] = gid
			}
		}
	}
	newConfig.Shards = newShards
	ca.Configs = append(ca.Configs, newConfig)
	return OK
}

func (ca *ConfigArray) Move(shard int, gid int) string {
	newConfig := ca.createConfig()
	// if group-gid exists, move shard in it
	if _, exist := newConfig.Groups[gid]; exist {
		newConfig.Shards[shard] = gid
		ca.Configs = append(ca.Configs, newConfig)
	}
	return OK
}

func (ca *ConfigArray) Query(num int) (Config, string) {
	if num < 0 || num >= len(ca.Configs) {
		return ca.Configs[len(ca.Configs)-1], OK
	}
	return ca.Configs[num], OK
}

func (ca *ConfigArray) createConfig() Config {
	// get latest config
	lastConfig := ca.Configs[len(ca.Configs)-1]
	// new config initialization
	newConfig := Config{lastConfig.Num + 1, lastConfig.Shards, make(map[int][]string)}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

func (ca *ConfigArray) groupByGid(cfg Config) map[int][]int {
	shardsCount := map[int][]int{}
	// create list of groups & number the groups randomly
	for gid, _ := range cfg.Groups {
		shardsCount[gid] = []int{}
	}
	// add every shard to different groups
	for shardIdx, gid := range cfg.Shards {
		shardsCount[gid] = append(shardsCount[gid], shardIdx)
	}
	return shardsCount
}

func (ca *ConfigArray) getMaxShardGid(shardsCount map[int][]int) int {
	// take from free group until all shards busy
	if shards, ok := shardsCount[freeGid]; ok && len(shards) > 0 {
		return freeGid
	}
	// make iteration deterministic
	var keys []int
	for k := range shardsCount {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(shardsCount[gid]) > max {
			index, max = gid, len(shardsCount[gid])
		}
	}
	return index
}

func (ca *ConfigArray) getMinShardGid(shardsCount map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range shardsCount {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != freeGid && len(shardsCount[gid]) < min {
			index, min = gid, len(shardsCount[gid])
		}
	}
	return index
}
