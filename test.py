import hashlib
from time import time

from cachedis import Cachedis

from redis import Redis


shards = [
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},

    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},

    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},

    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
    {"host": "127.0.0.1", "port": 6379, "db": 0},
]


d = dict( ("uid:%d" % i, hashlib.sha1("uid:%d" % i).hexdigest() * 500) for i in xrange(2000) )
cli = Cachedis(shards)

t0 = time()
cli.mset(d)
td = time() - t0
print td


t0 = time()
cli.mget(d)
td = time() - t0
print td


r = Redis(port=5354)

t0 = time()
r.mset(d)
td = time() - t0
print td


t0 = time()
r.mget(d)
td = time() - t0
print td
