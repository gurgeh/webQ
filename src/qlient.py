import cPickle

import requests

from backports import lzma

def read_pack(x, offset=0):
    ln = struct.unpack('q', x[offset:offset + 8])[0]
    return x[offset + 8:offset + 8 + ln], offset + 8 + ln

def read_key(x, offset=0):
    return read_pack(x, offset)[0]

def read_item(x, offset=0):
    key, offset = read_pack(offset)
    return key, x[offset:]

def read_elem(x, offset=0):
    key, offset = read_pack(x, offset)
    val, offset = read_pack(x, offset)
    return key, val, offset

def read_elems(x):
    offset = 0
    while offset < len(x):
        key, val, offset = read_elem(x, offset)
        yield key, val

def make_data(key, val, elem=False):
    s1 = struct.pack('q', len(key))
    s2 = ''
    if elem:
        s2 = struct.pack('q', len(val))
        
    return ''.join([s1, key, s2, val])

"""
db put

test
agent
remote_config

test
qrawler
"""

class Qlient:
    def __init__(self, host, queue, name, secret):
        self.host = host
        self.queue = queue
        self.secret = secret

    def call(self, method, path, params, files):
        params.update({'sig':self.secret, 'name':self.name})
        return getattr(requests, method)(self.host + path, params=params)

    def dbput(self, table, key, val):
        zval = lzma.compress(cPickle.dumps(items))
        self.call('put', '/db/%s' % table, {'key':key, 'value': zval})
    
    def put(self, key, val):
        zval = lzma.compress(val)
        self.call('put', '/q/%s' % self.queue, params={'key':key, 'value': zval})


    def put_n(self, items):
        zitems = lzma.compress(cPickle.dumps(items))
        self.call('put', '/mq/%s/' % self.queue, params={'items': zitems})
        
    def get(self):
        res = self.call('get', '/q/%s' % self.queue)
        key, val = read_item(res.content)
        return key, lzma.decompress(val)

    def get_n(self, n):
        res = self.call('get', '/mq/%s/%d' % (self.queue, n))
        for x in read_elems(res.content):
            yield x

    def qlen(self):
        res = self.call('get', '/lenq/%s' % self.queue)
        return int(res.text)
        
    def clean(self, key):
        self.call('delete', '/gone/', params={'key': key})

    def get_keys(self):
        res = self.call('get', '/gone/')
        return res.json()

    def nr_keys(self):
        res = self.call('get', '/nrgone/')
        return int(res.text)
