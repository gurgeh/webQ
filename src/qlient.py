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
"""

class Qlient:
    def __init__(self, host, queue, secret):
        self.host = host
        self.queue = queue
        self.secret = secret

    def put(self, key, val):
        zval = lzma.compress(val)
        requests.put('%s/q/%s' % (self.host, self.queue), params={'key':key, 'sig':self.secret, 'value': zval})

    def get(self):
        res = requests.get('%s/q/%s' % (self.host, self.queue), params={'sig':self.secret})
        key, val = read_item(res.content)
        return key, lzma.decompress(val)

    def get_n(self, n):
        res = requests.get('%s/q/%s/%d' % (self.host, self.queue, n), params={'sig':self.secret})
        for x in read_elems(res.content):
            yield x

    def qlen(self):
        res = requests.get('%s/lenq/%s' % (self.host, self.queue), params={'sig': self.secret})
        return int(res.text)
        
    def clean(self, key):
        requests.delete('%s/gone/' % (self.host,), files = {'key':key}, params={'sig': self.secret})

    def get_keys(self):
        res = requests.get('%s/gone/' % (self.host, ), params={'sig': self.secret})
        return res.json()

    def nr_keys(self):
        res = requests.get('%s/nrgone/' % (self.host, ), params={'sig': self.secret})
        return int(res.text)
