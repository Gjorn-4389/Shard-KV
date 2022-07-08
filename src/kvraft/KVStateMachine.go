package kvraft

import "fmt"

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
	Print() string
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Print() string {
	str := "map={"
	for k, v := range memoryKV.KV {
		str += fmt.Sprintf("\n%v:%v", k, v)
	}
	str += "}"
	return str
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}
