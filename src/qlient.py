import cPickle
import json

import requests

from backports import lzma

"""
unittests
"""

class Qlient:
    def __init__(self, host, queue, name, secret):
        self.host = host
        self.queue = queue
        self.name = name
        self.secret = secret

    def call(self, method, path, params=None):
        if params is None:
            params = {}
        params.update({'sig':self.secret, 'name':self.name})
        res = getattr(requests, method)(self.host + path, data=params)
        if res.status_code != 200:
            raise AssertionError, res.status_code
        return res

    def bigcall(self, method, path, data):
        res = getattr(requests, method)(self.host + path, data=data, headers={'content-type': 'application/lzma'}, params={'sig':self.secret, 'name':self.name})
        if res.status_code != 200:
            raise AssertionError, res.status_code
        return res
    
    def dbput(self, table, doc): # doc may be a list of docs
        zdoc = lzma.compress(cPickle.dumps(doc))
        self.bigcall('put', '/db/%s' % table, zdoc)

    def dbget(self, table, key, default=None):
        obj = self.call('get', '/db/%s' % table, {'key':key}).json()['obj']
        if not obj:
            obj = default
        return obj
        
    def put(self, items):
        zitems = lzma.compress(cPickle.dumps(items))
        self.bigcall('put', '/q/%s' % self.queue, data=zitems)
    
    def get(self, n=1):
        res = self.call('post', '/q/%s/%d' % (self.queue, n))
        return cPickle.loads(lzma.decompress(res.content))

    def qlen(self):
        res = self.call('get', '/lenq/%s' % self.queue)
        return res.json()['length']
        
    def clean(self, key):
        self.call('delete', '/gone/', params={'key': key})

    def clean_many(self, keys):
        self.call('delete', '/gone/', params={'keys': json.dumps(keys)})

    def get_keys(self):
        res = self.call('get', '/gone/')
        return res.json()['keys']

    def nr_keys(self):
        res = self.call('get', '/nrgone/')
        return res.json()['count']

    def log(self, fname, logs, skipped):
        self.call('put', '/log/%s'%fname, params={'logs': json.dumps((logs, skipped))})
        
    def qloop(self, n=1, backoff=60):
        while True:
            i = 0
            for x in self.get(n):
                yield x
                i += 1
            if i < n:
                time.sleep(backoff)
                
