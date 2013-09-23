import os
import re
import struct

from queuelib import FifoDiskQueue
import leveldb

from flask import Flask, Response
app = Flask(__name__)
app.config.from_envvar('WEBQ_SETTINGS')

QEXT = '.q'

def get_qfname(qname):
    return os.path.join(app.config['DATAPATH'], '%s.q' % qname)

def get_queues():
    qs = {}
    for q in os.listdir(app.config['DATAPATH']):
        if q.endswith(QEXT):
            qname = q[:-len(QEXT)]
            qs[qname] = FifoDiskQueue(get_qfname(qname))
    return qs

app.queues = get_queues()
app.seen = leveldb.Connect(os.path.join(app.config['DATAPATH'], 'seen.ldb'))

"""
lib
"""

OK_NAME = re.compile('[\w\d_-]+')

def read_key(x):
    ln = struct.unpack('q', x[:8])[0]
    return x[8:8+ln]

def make_data(key, val):
    s = struct.pack('q', len(key))
    return s + key + val

@app.route('/q/<queue>', methods=['GET'])
def get_queue(queue):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    if queue not in app.queues:
        abort(400)

    x = app.queues[queue].pop()
    if x is None:
        return ''

    key = read_key(x)
    app.seen.Put(key, 'r.%s' % queue)
    return Response(x, mimetype='application/octet-stream')
    

@app.route('/q/<queue>', methods=['PUT'])
def put_queue(queue):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    if queue not in app.queues:
        if not OK_NAME.match(queue):
            abort(403)
        app.queues[queue] = FifoDiskQueue(get_qfname(queue))

    app.seen.Put(key, 'w.%s' % queue)
    app.queues[queue].push(make_data(request.form['key'], request.files['value'].read()))

@app.route('/lenq/<queue>', methods=['GET']):
def len_queue(queue):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
        
    return len(app.queues[queue])
    
@app.route('/key/<key>', methods=['DELETE'])
def clean(key):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    app.seen.Delete(key)

@app.route('/key/', methods=['GET'])
def get_keys():
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return Response(json.dumps(list(app.seen.RangeIter())), mimetype='application/json')

@app.route('/seen/', methods=['GET'])
def get_nrkeys():
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return sum(1 for _ in app.seen.RangeIter())
