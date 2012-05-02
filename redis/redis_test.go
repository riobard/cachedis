package redis

import (
    "testing"
    "crypto/rand"
    "bytes"
)


func TestSetGet(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }
    v := make([]byte, 500)
    rand.Read(v)
    err = r.Set("hello", v)
    if err != nil {
        t.Fatal(err)
    }
    v2 := r.Get("hello")
    if bytes.Compare(v, v2) != 0 {
        t.Fatal("GET/SET different values")
    } 
}


func TestMsetMget(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }

    keys := []string{"foo", "bar", "spam", "egg"}
    m := map[string][]byte{}
    for _, key := range keys {
        v := make([]byte, 500)
        rand.Read(v)
        m[key] = v
    }

    if err := r.Mset(m); err != nil {
        t.Fatal(err)
    }

    m2 := r.Mget(keys...)
    for k := range m {
        if bytes.Compare(m[k], m2[k]) != 0 {
            t.Fatal("MGET/MSET returned different results")
        }
    }
}


func TestDel(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }

    keys := []string{"foo", "bar", "spam", "egg", "hello"}
    n := r.Del(keys...)
    if n != len(keys) {
        t.Fatal("DEL less keys than expected")
    }
}
