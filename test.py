import hashlib
from cachedis import Cachedis


shards = [
    {"host": "127.0.0.1", "port": 6400, "db": 0},
    {"host": "127.0.0.1", "port": 6400, "db": 1},
    {"host": "127.0.0.1", "port": 6400, "db": 2},
    {"host": "127.0.0.1", "port": 6400, "db": 3},

    {"host": "127.0.0.1", "port": 6401, "db": 0},
    {"host": "127.0.0.1", "port": 6401, "db": 1},
    {"host": "127.0.0.1", "port": 6401, "db": 2},
    {"host": "127.0.0.1", "port": 6401, "db": 3},

    {"host": "127.0.0.1", "port": 6402, "db": 0},
    {"host": "127.0.0.1", "port": 6402, "db": 1},
    {"host": "127.0.0.1", "port": 6402, "db": 2},
    {"host": "127.0.0.1", "port": 6402, "db": 3},

    {"host": "127.0.0.1", "port": 6403, "db": 0},
    {"host": "127.0.0.1", "port": 6403, "db": 1},
    {"host": "127.0.0.1", "port": 6403, "db": 2},
    {"host": "127.0.0.1", "port": 6403, "db": 3},
]


#cli.set("foo", "bar")
#print cli.get("foo")

d = dict( ("uid:%d" % i, hashlib.sha1("uid:%d" % i).hexdigest() * 1024) for i in xrange(4000) )

cli = Cachedis(shards)
cli.mset(d)
#cli.mget(d)


cli = Cachedis(shards[:1])
cli.mset(d)
#cli.mget(d)
