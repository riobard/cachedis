from hashlib import sha1

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
        for (i, ks) in d.items():
            vs = self._conn[i].mget(ks)
            for (k, v) in zip(ks, vs):
                if v is not None:
                    rs[k] = v
        return rs


    def mset(self, mapping):
        d = {}
        for k, v in mapping.items():
            i = self._pos(k)
            d.setdefault(i, {})[k] = v

        for (i, m) in d.items():
            self._conn[i].mset(m)
        return True
