package shardis

import (
    "testing"
    "crypto/rand"
    "bytes"
    "../redis"
)


var shards = []string{
    "localhost:6379",
    "localhost:6379",
    "localhost:6379",
    "localhost:6379",
}

var testKeys = [][]byte{
    []byte("shardis-test-key0"),
    []byte("shardis-test-key1"),
    []byte("shardis-test-key2"),
    []byte("shardis-test-key3"),
    []byte("shardis-test-key4"),
    []byte("shardis-test-key5"),
    []byte("shardis-test-key6"),
    []byte("shardis-test-key7"),
    []byte("shardis-test-key8"),
    []byte("shardis-test-key9"),
}


func TestSetGet(t *testing.T) {
    r, err := Open(shards...)
    if err != nil {
        t.Fatal(err)
    }
    v := make([]byte, 500)
    for _, k := range testKeys {
        rand.Read(v)
        err = r.Set(k, v)
        if err != nil {
            t.Fatal(err)
        }
        v2, err := r.Get(k)
        if err != nil {
            t.Fatal(err)
        }
        if bytes.Compare(v, v2) != 0 {
            t.Fatal("GET/SET different values")
        } 
    }
}


func TestMsetMget(t *testing.T) {
    r, err := Open(shards...)
    if err != nil {
        t.Fatal(err)
    }

    ps := make([]redis.KVPair, len(testKeys))
    for i, k := range testKeys {
        v := make([]byte, 500)
        rand.Read(v)
        ps[i].K = k
        ps[i].V = v
    }

    ks := r.Mset(ps)
    if len(ks) != len(ps) {
        t.Fatalf("Failed to MSET some keys")
    }

    ps2 := r.Mget(testKeys...)

    if len(ps) != len(ps2) {
        t.Fatal("MGET/MSET returned different results")
    }
}


func TestDel(t *testing.T) {
    r, err := Open(shards...)
    if err != nil {
        t.Fatal(err)
    }

    n, err := r.Del(testKeys...)
    if err != nil {
        t.Fatal(err)
    }
    if n != len(testKeys) {
        t.Fatal("DEL less keys than expected")
    }
}



/*
func BenchmarkMset(b *testing.B) {
    b.StopTimer()

    r, _ := Open(shards...)
    m := map[string][]byte{}
    for i := 0; i < 1000; i++ {
        k := fmt.Sprintf("benchmark-key-%d", i)
        v := make([]byte, 500)
        rand.Read(v)
        m[k] = v
    }

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Mset(m)
    }
    b.StopTimer()
}

func BenchmarkMget(b *testing.B) {
    b.StopTimer()

    r, _ := Open(shards...)
    keys := make([]string, 1000)
    for i := 0; i < 1000; i++ {
        keys[i] = fmt.Sprintf("benchmark-key-%d", i)
    }

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Mget(keys...)
    }
    b.StopTimer()
}

func BenchmarkSet(b *testing.B) {
    b.StopTimer()

    r, _ := Open(shards...)
    v := make([]byte, 500)
    rand.Read(v)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Set(fmt.Sprintf("benchmark-key-%d", i), v)
    }
    b.StopTimer()
}

func BenchmarkGet(b *testing.B) {
    b.StopTimer()

    r, _ := Open(shards...)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Get(fmt.Sprintf("benchmark-key-%d", i))
    }
    b.StopTimer()
}
*/
