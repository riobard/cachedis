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
    ks := make([]string, len(m))
    i := 0
    for k := range m {
        ks[i] = k
        i++
    }
    return ks
}

// Locate the ID of shard containing a key
func (c Shardis) locate(k string) uint16 {
    h := sha1.New()
    io.WriteString(h, k)
    s := h.Sum(nil)
    pos := ((uint16(s[0])<<0)|(uint16(s[1])<<8)) % uint16(len(c.shards))
    return pos
}

// Locate the IDs of shard containting the keys
func (c Shardis) locateKeys(ks... string) [][]string {
    res := make([][]string, len(c.shards))
    for _, k := range ks {
        loc := c.locate(k)
        res[loc] = append(res[loc], k)
    }
    return res
}


func (c Shardis) Set(k string, v []byte) error {
    return c.shards[c.locate(k)].Set(k, v)
}

func (c Shardis) Get(k string) ([]byte, error) {
    return c.shards[c.locate(k)].Get(k)
}

type intErrorPair struct {
    n int
    err error
}

func (c Shardis) Del(ks... string) (n int, err error) {
    if len(ks) == 0 {
        return 0, nil
    }
    t := c.locateKeys(ks...)
    ch := make(chan intErrorPair, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, ks []string) {
            n, err := shard.Del(ks...)
            ch <- intErrorPair{n, err}
        }(shard, t[i])
    }

    for i := 0; i < len(c.shards); i++ {
        pair := <- ch
        n += pair.n
        err = pair.err
    }
    return n, err
}

type mapErrorPair struct {
    m map[string][]byte
    err error
}

func (c Shardis) Mget(ks... string) (m map[string][]byte, err error) {
    if len(ks) == 0 {
        return m, nil
    }

    t := c.locateKeys(ks...)
    ch := make(chan mapErrorPair, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, ks []string) {
            m, err := shard.Mget(ks...)
            ch <- mapErrorPair{m, err}
        }(shard, t[i])
    }

    m = make(map[string][]byte, len(ks))
    for i := 0; i < len(c.shards); i++ {
        pair := <- ch
        for k, v := range pair.m {
            m[k] = v
        }
        err = pair.err
    }
    return m, err
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
