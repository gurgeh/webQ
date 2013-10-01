import cPickle
import os
import re
import json

from backports import lzma
from pymongo import MongoClient

from flask import Flask, jsonify, request, make_response
app = Flask(__name__)
app.config.from_envvar('WEBQ_SETTINGS')


QEXT = '.q'

"""
logging
"""

app.db = MongoClient().webQ
app.gone = app.db['_gone']

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

    ret = []
    for _ in xrange(int(n)):
        x = app.db[queue].find_one()
        if x is None:
            break

        idx = {'_id': x['_id']}
        app.gone.update(idx, x, upsert=True)
        app.db[queue].remove(idx)
        
        ret.append(x)

    res = make_response(lzma.compress(cPickle.dumps(ret)))
    res.mimetype = 'application/octet-stream'
    return res


@app.route('/q/<queue>', methods=['PUT'])
def put_queue(queue):
    if request.args['sig'] != app.config['SECRET']:
        abort(403)

    if not OK_NAME.match(queue):
        abort(403)

    items = cPickle.loads(lzma.decompress(request.data))

    for x in items:
        idx = {'_id': x['_id']}
        app.gone.remove(idx)
        app.db[queue].update(idx, x, upsert=True)

    return ''


@app.route('/lenq/<queue>', methods=['GET'])
def len_queue(queue):
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
        
    return jsonify(length=app.db[queue].count())

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
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return jsonify(keys=[x['_id'] for x in app.gone.find()])

@app.route('/nrgone/', methods=['GET'])
def get_nrkeys():
    if request.form['sig'] != app.config['SECRET']:
        abort(403)
    
    return jsonify(count=app.gone.count())

if __name__ == '__main__':
    app.debug = True
    app.run()

        
