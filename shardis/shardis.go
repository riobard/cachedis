// Redis client with sharding

package shardis

import (
	"../redis"
	"crypto/sha1"
	"io"
)

type Shardis struct {
    shards []*redis.Redis
}


func Open(shardsAddr []string) (*Shardis, error) {
    c := &Shardis{}
    for _, addr := range shardsAddr {
        r, err := redis.Open(addr)
        if err != nil {
            return nil, err
        }
        c.shards = append(c.shards, r)
    }
    return c, nil
}


// Get the keys of a map
func keys(m map[string][]byte) []string {
    keys := make([]string, len(m))
    i := 0
    for k := range m {
        keys[i] = k
        i++
    }
    return keys
}

// Locate the ID of shard containing a key
func (c Shardis) locate(key string) uint16 {
    h := sha1.New()
    io.WriteString(h, key)
    s := h.Sum(nil)
    pos := ((uint16(s[0])<<0)|(uint16(s[1])<<8)) % uint16(len(c.shards))
    return pos
}

// Locate the IDs of shard containting the keys
func (c Shardis) locateKeys(keys... string) [][]string {
    res := make([][]string, len(c.shards))
    for _, k := range keys {
        loc := c.locate(k)
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

func (c Shardis) Del(keys... string) int {
    if len(keys) == 0 {
        return 0
    }
    t := c.locateKeys(keys...)
    ch := make(chan int, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, keys []string) {
            ch <- shard.Del(keys...)
        }(shard, t[i])
    }

    n := 0
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
        go func(i int, shard *redis.Redis) {
            // subset of m belonging to a shard
            n := make(map[string][]byte, len(t[i]))
            for _, k := range t[i] {
                n[k] = m[k]
            }

            err := shard.Mset(n)
            res := make(map[string]error, len(n))
            for k := range n {
                res[k] = err
            }
            ch <- res
        }(i, shard)
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
