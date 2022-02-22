from asyncio.log import logger
import os
from bottle import route, run, request, get, post
from database import KVDatabase

db = KVDatabase()

@route('/get', method='GET')
def get():
    key = request.query.key
    logger.info(f"Get: {key}")
    value = db.get(key)
    return value

@post('/put', method='POST')
def put():
    data = request.json
    json_keys = ['key', 'value']
    key, value = [data.get(k) for k in json_keys]
    value = db.put(key, value)
    return value

@route('/delete', method='DELETE')
def delete():
    data = request.json
    key = data.get('key')
    value = db.delete(key)
    return value

@route('/update', method='POST')
def update():
    data = request.json
    json_keys = ['key', 'value']
    key, value = [data.get(k) for k in json_keys]
    value = db.update(key, value)
    return value

@route('/health', method='GET')
def health():
    return 'OK'

NULL_HOST = os.environ.get("NULL_HOST", "localhost")
NULL_PORT = os.environ.get("NULL_PORT", 4567)
run(host=NULL_HOST, port=NULL_PORT)