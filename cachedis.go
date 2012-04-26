package main

import (
	"./redis"
	"./shardis"
	"flag"
	"fmt"
	"time"
)

var addr = flag.String("addr", "localhost:6379", "http service address")


type Cachedis struct {
    shards []*redis.Redis
}


func Open(shardsConfig []string) *Cachedis {
    c := &Cachedis{}
    return c
}

func (c Cachedis) Set(key string, value []byte) error {
    return nil
}


func init() {
	flag.Parse()
}

func test_redis() {

	r, err := redis.Open(*addr)
	if err != nil {
		fmt.Println("Failed to connect")
		return
	}
	r.Mset(map[string][]byte{
		"foo":  []byte("bar"),
		"spam": []byte("egg"),
	})
	m := r.Mget("foo", "spam", "hello", "blah", "ahah")
	fmt.Printf("===> %q\n", m)

	r.Set("hello", []byte(""))
	res1 := r.Get("hello")
	if res1 != nil {
		fmt.Printf("hello = %q\n", res1)
	} else {
		fmt.Printf("hello = nil\n")
	}

	res2 := r.Get("hell")
	if res2 != nil {
		fmt.Printf("hell = %q\n", res2)
	} else {
		fmt.Printf("hell = nil\n")
	}
}


func main() {
    c := shardis.Open([]string{
        "debian2.zhq:6400", 
        "debian2.zhq:6400", 

        "debian2.zhq:6401",
        "debian2.zhq:6401",

        "debian2.zhq:6402",
        "debian2.zhq:6402",

        "debian2.zhq:6403",
        "debian2.zhq:6403",
    })

    N := 500
    m := make(map[string][]byte, N)

    keys := []string{}
    for i := 0; i < N; i++ {
        k := fmt.Sprintf("uid:%d", i)
        keys = append(keys, k)
        v := make([]byte, 40<<10)
        m[k] = v
    }

    msett0 := time.Now().UnixNano()
    msetres := c.Mset(m)
    msettd := time.Now().UnixNano() - msett0
    fmt.Printf("ParaMget (%d): %f\n", len(msetres), float64(msettd)/1e6)

    mgett0 := time.Now().UnixNano()
    mgetres := c.Mget(keys...)
    mgettd := time.Now().UnixNano() - mgett0
    fmt.Printf("ParaMget (%d): %f\n", len(mgetres), float64(mgettd)/1e6)

}
