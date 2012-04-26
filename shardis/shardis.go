package shardis

import (
	"../redis"
	"io"
	"crypto/sha1"
	"log"
)




type Shardis struct {
    shards []*redis.Redis
}


func Open(shardsConfig []string) *Shardis {
    c := &Shardis{}
    var r *redis.Redis
    for _, cfg := range shardsConfig {
        r, _ = redis.Open(cfg)
        c.shards = append(c.shards, r)
    }
    return c
}

func (c Shardis) pos(key string) uint16 {
    h := sha1.New()
    io.WriteString(h, key)
    s := h.Sum(nil)
    pos := ((uint16(s[0])<<0)|(uint16(s[1])<<8)) % uint16(len(c.shards))
    return pos
}

func (c Shardis) Set(key string, value []byte) error {
    return c.shards[c.pos(key)].Set(key, value)
}

func (c Shardis) Get(key string) []byte {
    return c.shards[c.pos(key)].Get(key)
}

func (c Shardis) Mget(keys... string) map[string][]byte {
    t := make([][]string, len(c.shards))
    var pos uint16
    for _, k := range keys {
        pos = c.pos(k)
        t[pos] = append(t[pos], k)
    }
    m := make(map[string][]byte)
    for i := 0; i < len(c.shards); i++ {
        res := c.shards[i].Mget(t[i]...)
        log.Printf("Shard%d = %d\n", i, len(res))
        for k, v := range res {
            m[k] = v
        }
    }
    return m
}

func (c Shardis) ParaMget(keys... string) map[string][]byte {
    t := make([][]string, len(c.shards))
    var pos uint16
    for _, k := range keys {
        pos = c.pos(k)
        t[pos] = append(t[pos], k)
    }
    m := make(map[string][]byte)
    ch := make(chan map[string][]byte, len(c.shards))
    for i, shard := range c.shards {
        go func(i uint, shard *redis.Redis) {
            m := shard.Mget(t[i]...)
            ch <- m
        }(uint(i), shard)
    }

    for i := 0; i < len(c.shards); i++ {
        each := <- ch
        log.Printf("Shard%d = %d\n", i, len(each))
        for k, v := range each {
            m[k] = v
        }
    }
    return m
}
