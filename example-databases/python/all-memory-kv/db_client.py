import os
import sys
import requests

NULL_HOST = os.environ.get("NULL_HOST", "localhost")
NULL_PORT = os.environ.get("NULL_PORT", 4567)

DB_URL = f'http://{NULL_HOST}:{NULL_PORT}'

actions = {
    'get': DB_URL + '/get',
    'put': DB_URL + '/put',
    'delete': DB_URL + '/delete',
    'update': DB_URL + '/update',
}

def send_db_request(command:str, key: str, value:str=None):
    if command not in actions.keys():
        raise ValueError('Command not found.')
    
    url = actions.get(command)
    if command == 'get':
        response = requests.get(url, params={'key': key})
    elif command == 'delete':
        response = requests.delete(url, json={'key': key})
    else:
        response = requests.post(url, json={'key':key, 'value': value})
    return response.text 

def parse_input(request:str):
    command, key, *value = request.split(' ')
    command = command.lower()
    value = ' '.join(value)
    response = send_db_request(command, key, value)
    print(response)

# Check if db server is running!
try:
    conn = requests.get(f"{DB_URL}/health")
    if conn.status_code != 200:
        raise ConnectionError("Cannot Connect to db.")
    else:
        print(f"Connected to db: {DB_URL}")
except requests.exceptions.RequestException:
    raise ConnectionError("Cannot connect to db at: {DB_URL}")

while True:
    try:
        user_input = input('$ ')
        parse_input(user_input)
    except KeyboardInterrupt:
        break

print("Qutting db.")
sys.exit(0)