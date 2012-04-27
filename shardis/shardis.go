package shardis

import (
	"../redis"
	"crypto/sha1"
	"io"
)

type Shardis struct {
    shards []*redis.Redis
}


func Open(shardsAddr []string) *Shardis {
    c := &Shardis{}
    var r *redis.Redis
    for _, addr := range shardsAddr {
        r, _ = redis.Open(addr)
        c.shards = append(c.shards, r)
    }
    return c
}


func keys(m map[string][]byte) []string {
    keys := make([]string, len(m))
    i := 0
    for k := range m {
        keys[i] = k
        i++
    }
    return keys
}

func (c Shardis) locate(key string) uint16 {
    h := sha1.New()
    io.WriteString(h, key)
    s := h.Sum(nil)
    pos := ((uint16(s[0])<<0)|(uint16(s[1])<<8)) % uint16(len(c.shards))
    return pos
}

func (c Shardis) locateKeys(keys... string) [][]string {
    res := make([][]string, len(c.shards))
    var loc uint16
    for _, k := range keys {
        loc = c.locate(k)
        res[loc] = append(res[loc], k)
    }
    return res
}


func (c Shardis) Set(key string, value []byte) error {
    return c.shards[c.locate(key)].Set(key, value)
}

func (c Shardis) Get(key string) []byte {
    return c.shards[c.locate(key)].Get(key)
}

func (c Shardis) Del(keys... string) int64 {
    if len(keys) == 0 {
        return 0
    }
    t := c.locateKeys(keys...)
    ch := make(chan int64, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, keys []string) {
            ch <- shard.Del(keys...)
        }(shard, t[i])
    }

    n := int64(0)
    for i := 0; i < len(c.shards); i++ {
        n += <- ch
    }
    return n
}

func (c Shardis) Mget(keys... string) map[string][]byte {
    if len(keys) == 0 {
        return nil
    }

    t := c.locateKeys(keys...)
    ch := make(chan map[string][]byte, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, keys []string) {
            ch <- shard.Mget(keys...)
        }(shard, t[i])
    }

    m := make(map[string][]byte, len(keys))
    for i := 0; i < len(c.shards); i++ {
        res := <- ch
        for k, v := range res {
            m[k] = v
        }
    }
    return m
}


func (c Shardis) Mset(m map[string][]byte) map[string]error {
    if len(m) == 0 {
        return nil
    }

    t := c.locateKeys(keys(m)...)
    ch := make(chan map[string]error, len(c.shards))
    for i, shard := range c.shards {
        n := make(map[string][]byte, len(t[i]))
        for _, k := range t[i] {
            n[k] = m[k]
        }

        go func(shard *redis.Redis, n map[string][]byte) {
            err := shard.Mset(n)
            res := make(map[string]error, len(n))
            for k, _ := range(n) {
                res[k] = err
            }
            ch <- res
        }(shard, n)
    }

    all := make(map[string]error, len(m))
    for i := 0; i < len(c.shards); i++ {
        res := <- ch
        for k, v := range res {
            all[k] = v
        }
    }
    return all
}
