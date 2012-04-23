from hashlib import sha1
from time import time

import redis



class Cachedis(object):

    def __init__(self, shards):
        self._conn = [redis.Redis(**cfg) for cfg in shards]
        self._N = len(self._conn)


    def _pos(self, key):
        return int(sha1(key).hexdigest()[:8], 16) % self._N


    def get(self, key):
        return self._conn[self._pos(key)].get(key)


    def set(self, key, value):
        return self._conn[self._pos(key)].set(key, value)


    __getitem__ = get
    __setitem__ = set


    def mget(self, keys, *args):
        d = {}
        for k in keys:
            i = self._pos(k)
            d.setdefault(i, []).append(k)

        rs = {}
        tt0 = time()
        for (i, ks) in d.items():
            t0 = time()
            vs = self._conn[i].mget(ks)
            for (k, v) in zip(ks, vs):
                if v is not None:
                    rs[k] = v
            td = time() - t0
            print td
        ttd = time() - tt0
        print ttd
        return rs


    def mset(self, mapping):
        d = {}
        for k, v in mapping.items():
            i = self._pos(k)
            d.setdefault(i, {})[k] = v

        
        tt0 = time()
        for (i, m) in d.items():
            t0 = time()
            self._conn[i].mset(m)
            td = time() - t0
            print i, td
        ttd = time() - tt0
        print ttd

        return True
