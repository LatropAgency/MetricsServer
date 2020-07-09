import ast
import socket
import time


class ClientError(Exception):
    pass


class Client:
    def __init__(self, host, port, timeout=1):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = socket.create_connection((self.host, self.port), timeout=timeout)

    def get(self, name: str):
        if not isinstance(name, str):
            raise ClientError
        query = f'get {name}\n'
        self.socket.send(query.encode('utf-8'))
        data = self.socket.recv(4096)
        response = data.decode('utf-8')
        result = {}
        lst = response.split('\n')
        if lst[0] == 'error':
            raise ClientError
        del (lst[0])
        try:
            for item in lst:
                if item == '':
                    break
                items = item.split()
                tmp = result.get(items[0], [])
                tmp.append((int(items[2]), float(items[1])))
                tmp.sort(key=lambda x: x[0])
                result[items[0]] = tmp
        except:
            raise ClientError
        return result

    def put(self, name, value, timestamp=None):
        if timestamp is None:
            timestamp = int(time.time())
        if not (isinstance(timestamp, int) or isinstance(value, float)):
            raise ClientError
        self.socket.send((f'put {name} {value} {timestamp}\n').encode('utf-8'))
        data = self.socket.recv(1024)
        response = data.decode('utf-8')
        if response.startswith('error'):
            raise ClientError
