// Redis client with sharding

package shardis

import (
	"../redis"
	"crypto/sha1"
)

type Shardis struct {
    shards []*redis.Redis
}


func Open(shardsAddr ...string) (*Shardis, error) {
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


// Locate the ID of shard containing a key
func (c Shardis) locate(k []byte) uint16 {
    h := sha1.New()
    for len(k) > 0 {
        n, err := h.Write(k)
        if err != nil {
            panic(err)
        }
        k = k[n:]
    }
    s := h.Sum(nil)
    pos := ((uint16(s[0])<<0)|(uint16(s[1])<<8)) % uint16(len(c.shards))
    return pos
}

// Locate the IDs of shard containting the keys
func (c Shardis) locateKeys(ks ...[]byte) [][][]byte {
    res := make([][][]byte, len(c.shards))
    for _, k := range ks {
        loc := c.locate(k)
        res[loc] = append(res[loc], k)
    }
    return res
}

func (c Shardis) locatePairs(ps ...redis.KVPair) [][]redis.KVPair {
    res := make([][]redis.KVPair, len(c.shards))
    for _, p := range ps {
        loc := c.locate(p.K)
        res[loc] = append(res[loc], p)
    }
    return res
}


func (c Shardis) Set(k, v []byte) error {
    return c.shards[c.locate(k)].Set(k, v)
}

func (c Shardis) Get(k []byte) ([]byte, error) {
    return c.shards[c.locate(k)].Get(k)
}

type intErrorPair struct {
    n int
    err error
}

func (c Shardis) Del(ks ...[]byte) (n int, err error) {
    if len(ks) == 0 {
        return 0, nil
    }
    t := c.locateKeys(ks...)
    ch := make(chan intErrorPair, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, ks [][]byte) {
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


func (c Shardis) Mget(ks ...[]byte) []redis.KVPair {
    if len(ks) == 0 {
        return nil
    }

    t := c.locateKeys(ks...)
    ch := make(chan []redis.KVPair, len(c.shards))
    for i, shard := range c.shards {
        go func(shard *redis.Redis, ks [][]byte) {
            vs, err := shard.Mget(ks...)
            if err == nil {
                ps := make([]redis.KVPair, len(ks))
                for i, k := range ks {
                    ps[i].K = k
                    ps[i].V = vs[i]
                }
                ch <- ps
            } else {
                ch <- nil
            }
        }(shard, t[i])
    }

    ps := make([]redis.KVPair, len(ks))
    for i, j := 0, 0; i < len(c.shards) && j < len(ks); i++ {
        pairs := <- ch
        for _, v := range pairs {
            ps[j] = v
            j++
        }
    }
    return ps
}


func (c Shardis) Mset(ps []redis.KVPair) [][]byte {
    if len(ps) == 0 {
        return nil
    }

    parts := c.locatePairs(ps...)
    ch := make(chan int, len(c.shards))
    for i, shard := range c.shards {
        go func(i int, pairs []redis.KVPair, shard *redis.Redis) {
            err := shard.Mset(pairs...)
            if err == nil {
                ch <- i
            } else {
                ch <- -1
            }
        }(i, parts[i], shard)
    }

    ks := make([][]byte, 0)
    for i := 0; i < len(c.shards); i++ {
        if id := <- ch; id >= 0 {
            for _, p := range parts[id] {
                ks = append(ks, p.K)
            }
        }
    }
    return ks
}
