import cPickle
import os
import re
import json

from backports import lzma
from queuelib import FifoDiskQueue
from pymongo import MongoClient

from qlient import make_data, read_item

from flask import Flask, Response, jsonify, request
app = Flask(__name__)
app.config.from_envvar('WEBQ_SETTINGS')


QEXT = '.q'

"""
logging
"""

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
app.db = MongoClient().webQ
app.gone = app.db['gone']

OK_NAME = re.compile('[\w\d_-]+')

@app.route('/db/<table>', methods=['PUT'])
def put_db(table):
    if request.args['sig'] != app.config['SECRET']:
        abort(403)

    doc = cPickle.loads(lzma.decompress(request.data))

    app.db[table].insert(doc) #doc may be a list

    return ''

@app.route('/db/<table>', methods=['GET'])
def get_db(table):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    key = request.form['key']

    return jsonify(**app.db[table].find_one({"_id": key}))


@app.route('/q/<queue>/<n>', methods=['POST'])
def getn_queue(queue, n=1):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    if queue not in app.queues:
        abort(400)

    ret = []
    for _ in xrange(int(n)):
        x = app.queues[queue].pop()
        if x is None:
            break
        key,val = read_item(x)
        app.gone.insert({'_id':key, 'data':x})
        ret.append(make_data(key, val, True))

    return Response(''.join(x), mimetype='application/octet-stream')


@app.route('/q/<queue>', methods=['PUT'])
def put_queue(queue):
    if request.args['sig'] != app.config['SECRET']:
        abort(403)

    if queue not in app.queues:
        if not OK_NAME.match(queue):
            abort(403)
        app.queues[queue] = FifoDiskQueue(get_qfname(queue))

    #if 'key' in request.form:
    #    items = [(request.form['key'], request.form['value'])]
    #else:
    items = cPickle.loads(lzma.decompress(request.data))

    for key, val in items:
        app.gone.remove({'_id':key})
        app.queues[queue].push(make_data(key, val))

    return ''

        
@app.route('/lenq/<queue>', methods=['GET'])
def len_queue(queue):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
        
    return jsonify(length=len(app.queues[queue]))
    
@app.route('/gone/', methods=['DELETE'])
def clean():
    if request.form['sig'] != app.config['SECRET']:
        abort(403)

    if 'key' in request.form:
        keys = [request.form['key']]
    else:
        keys = request.form['keys']

    for key in keys:
        app.gone.remove({'_id': key})

    return ''

@app.route('/gone/', methods=['GET'])
def get_keys():
    print request.form
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return jsonify(**dict([read_item(x['data']) for x in app.gone.find()]))

@app.route('/nrgone/', methods=['GET'])
def get_nrkeys():
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return jsonify(count=app.gone.count())

if __name__ == '__main__':
    app.debug = True
    app.run()

        
