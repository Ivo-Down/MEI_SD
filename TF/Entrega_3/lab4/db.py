
import logging
from asyncio import Lock, sleep
from types import SimpleNamespace as sn
from random import random

class BaseDB:
    def __init__(self):
        self.data = {}
        self.locks = {}

    async def begin(self, keys, id):
        locks = list(set(keys))
        locks.sort()
        for k in locks:
            logging.debug('%s: locking %s', id, str(k))
            if k in self.locks:
                l = self.locks[k]
            else:
                l = Lock()
                self.locks[k] = l
            await l.acquire()

        return sn(tid=id,locked=locks)

    async def execute(self, ctx, txn):
        rs = []
        delta = {}
        res = []
        for op,k,v in txn:
            rs.append(k)
            cv = delta.get(k,self.data.get(k,None))
            if op == 'r':
                logging.debug('%s: reading %d', ctx.tid, k)
                res.append((op,k,cv))
            elif op == 'append':
                logging.debug('%s: appending to %d: %d', ctx.tid, k, v)
                if not cv:
                    cv = []
                cv = cv + [v]
                delta[k] = cv
                res.append((op,k,v))

        wv = list(delta.items())
        wv.sort()
        return (rs,wv,res)

    async def commit(self, ctx, wv):
        for k,v in wv:
            logging.debug('%s: updating %d: %s', ctx.tid, k, str(v))
            self.data[k] = v

    def cleanup(self, ctx):
        for k in ctx.locked:
            logging.debug('%s: unlocking %d', ctx.tid, k)
            self.locks[k].release()

class DB(BaseDB):
    def __init__(self, det=False):
        super().__init__()
        self.det = det

    async def execute(self, ctx, txn):
        await sleep(0.01*len(txn))
        if not self.det and random()<0.1:
            logging.info('%s: aborted', ctx.tid)
            return (None,None,None)
        return await super().execute(ctx, txn)

    async def commit(self, ctx, wv):
        await sleep(0.1)
        await super().commit(ctx, wv)