package shardis

import (
    "testing"
    "crypto/rand"
    "bytes"
    "fmt"
)


var shards = []string{
    "localhost:6379",
    "localhost:6379",
    "localhost:6379",
    "localhost:6379",
}

var testKeys = []string{
    "shardis-test-key0",
    "shardis-test-key1",
    "shardis-test-key2",
    "shardis-test-key3",
    "shardis-test-key4",
    "shardis-test-key5",
    "shardis-test-key6",
    "shardis-test-key7",
    "shardis-test-key8",
    "shardis-test-key9",
}


func TestSetGet(t *testing.T) {
    r, err := Open(shards)
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
        v2 := r.Get(k)
        if bytes.Compare(v, v2) != 0 {
            t.Fatal("GET/SET different values")
        } 
    }
}


func TestMsetMget(t *testing.T) {
    r, err := Open(shards)
    if err != nil {
        t.Fatal(err)
    }

    m := map[string][]byte{}
    for _, key := range testKeys {
        v := make([]byte, 500)
        rand.Read(v)
        m[key] = v
    }

    rs := r.Mset(m)
    for key, err := range rs {
        if err != nil {
            t.Fatalf("Failed to MSET key %q. Error: %q\n", key, err)
        }
    }

    m2 := r.Mget(testKeys...)
    for k := range m {
        if bytes.Compare(m[k], m2[k]) != 0 {
            t.Fatal("MGET/MSET returned different results")
        }
    }
}


func TestDel(t *testing.T) {
    r, err := Open(shards)
    if err != nil {
        t.Fatal(err)
    }

    n := r.Del(testKeys...)
    if n != len(testKeys) {
        t.Fatal("DEL less keys than expected")
    }
}



func BenchmarkMset(b *testing.B) {
    b.StopTimer()

    r, _ := Open(shards)
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

    r, _ := Open(shards)
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

    r, _ := Open(shards)
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

    r, _ := Open(shards)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Get(fmt.Sprintf("benchmark-key-%d", i))
    }
    b.StopTimer()
}
