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
    //m := make(map[string][]byte)

    keys := []string{}
    for i := 0; i < N; i++ {
        k := fmt.Sprintf("uid:%d", i)
        keys = append(keys, k)
        //v := []byte(fmt.Sprintf("%d", i))
        v := make([]byte, 40<<10)
        //m[k] = v
        c.Set(k, v)
        /*
        v := c.Get(k)
        if false {
            if v == nil {
                fmt.Printf("%q = nil\n", k)
            } else {
                fmt.Printf("%q = %q\n", k, c.Get(k))
            }
        }*/
    }

    t0 := time.Now().UnixNano()
    res1 := c.Mget(keys...)
    td := time.Now().UnixNano() - t0
    fmt.Printf("Mget (%d): %f\n", len(res1), float64(td)/1e6)

    parat0 := time.Now().UnixNano()
    res2 := c.ParaMget(keys...)
    paratd := time.Now().UnixNano() - parat0
    fmt.Printf("ParaMget (%d): %f\n", len(res2), float64(paratd)/1e6)


}
