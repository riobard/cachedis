package redis

import (
    "testing"
    "crypto/rand"
    "bytes"
    "bufio"
)



func TestParseLine(t *testing.T) {
    msg := bufio.NewReader(bytes.NewBufferString("+OK\r\n"))
    m, err := Parse(msg)
    if err != nil {
        t.Fatal(err)
    }

    if m.Kind != '+' || string(m.Value) != "OK" {
        t.Fatal("parsing error")
    }
}

func TestParseInteger(t *testing.T) {
    msg := bufio.NewReader(bytes.NewBufferString(":42\r\n"))
    m, err := Parse(msg)
    if err != nil {
        t.Fatal(err)
    }

    if m.Kind != ':' || string(m.Value) != "42" || m.Integer != 42 {
        t.Fatal("parsing error")
    }
}


func TestParseBulk(t *testing.T) {
    msg := bufio.NewReader(bytes.NewBufferString("$5\r\nhello\r\n"))
    m, err := Parse(msg)
    if err != nil {
        t.Fatal(err)
    }

    if m.Kind != '$' || string(m.Value) != "hello" {
        t.Fatal("parsing error")
    }
}

func TestParseMulti(t *testing.T) {
    msg := bufio.NewReader(bytes.NewBufferString("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
    m, err := Parse(msg)
    if err != nil {
        t.Fatal(err)
    }

    if m.Kind != '*' || len(m.Values) != 2 || string(m.Values[0]) != "GET" || string(m.Values[1]) != "foo" {
        t.Fatal("parsing error")
    }
}


func TestEncodeLine(t *testing.T) {
    m := &Message{Kind: '+', Value: []byte("OK")}
    b, err := Encode(m)
    if err != nil {
        t.Fatal(err)
    }
    if string(b) != "+OK\r\n" {
        t.Fatal("encoding error")
    }
}

func TestEncodeBulk(t *testing.T) {
    m := &Message{Kind: '$', Value: []byte("foobar")}
    b, err := Encode(m)
    if err != nil {
        t.Fatal(err)
    }
    if string(b) != "$6\r\nfoobar\r\n" {
        t.Fatal("encoding error")
    }
}


func TestEncodeMulti(t *testing.T) {
    m := &Message{Kind: '*'}
    m.Values = make([][]byte, 2)
    m.Values[0] = []byte("GET")
    m.Values[1] = []byte("bar")
    b, err := Encode(m)
    if err != nil {
        t.Fatal(err)
    }
    s := "*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n"
    if string(b) != s {
        t.Fatalf("encoding error: expecting %q, got %q", s, b)
    }
}


func TestSetGet(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }
    v := make([]byte, 500)
    rand.Read(v)
    err = r.Set([]byte("hello"), v)
    if err != nil {
        t.Fatal(err)
    }
    v2, err := r.Get([]byte("hello"))
    if err != nil {
        t.Fatal(err)
    }
    if bytes.Compare(v, v2) != 0 {
        t.Fatal("GET/SET different values")
    } 
}


func TestMsetMget(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }

    ks := [][]byte{[]byte("foo"), 
                   []byte("bar"), 
                   []byte("spam"), 
                   []byte("egg")}

    ps := make([]KVPair, len(ks))

    for i, k := range ks {
        v := make([]byte, 500)
        rand.Read(v)
        ps[i] = KVPair{K: k, V: v}
    }

    if err := r.Mset(ps...); err != nil {
        t.Fatal(err)
    }

    vs, err := r.Mget(ks...)
    if err != nil {
        t.Fatal(err)
    }
    for i := 0; i < len(ks); i++ {
        if bytes.Compare(ps[i].V, vs[i]) != 0 {
            t.Fatal("MGET/MSET returned different results")
        }
    }
}


func TestDel(t *testing.T) {
    r, err := Open("")
    if err != nil {
        t.Fatal(err)
    }

    ks := [][]byte{[]byte("foo"), 
                   []byte("bar"), 
                   []byte("spam"), 
                   []byte("egg"), 
                   []byte("hello")}

    n, err := r.Del(ks...)
    if err != nil {
        t.Fatal(err)
    }
    if n != len(ks) {
        t.Fatal("DEL less keys than expected")
    }
}
