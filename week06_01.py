import socket
from threading import Thread, RLock


class Source():
    def __init__(self):
        self.__source = {}
        self.lock = RLock()

    def get(self, name):
        with self.lock:
            if name == '*':
                return self.get_all()
            else:
                items = self.__source.get(name, [])
                result = 'ok\n'
                for item in items:
                    result += f'{name} {item[0]} {item[1]}\n'
                result += '\n'
                return result

    def put(self, name, value, timestamp):
        with self.lock:
            items = self.__source.get(name, [])
            if (value, timestamp) not in items:
                for item in items:
                    if timestamp in item:
                        items.remove(item)
                        items.append((value, timestamp))
                        break
                else:
                    items.append((value, timestamp))
                self.__source[name] = items
            return 'ok\n\n'

    def get_all(self):
        with self.lock:
            result = 'ok\n'
            for key in self.__source:
                items = self.__source.get(key)
                for item in items:
                    result += f'{key} {item[0]} {item[1]}\n'
            result += '\n'
            return result


def run_server(host, port):
    server = socket.socket()
    server.bind((host, port))
    server.listen(socket.SOMAXCONN)
    source = Source()
    while True:
        conn, addr = server.accept()
        thread = Thread(target=answer, args=(conn, source))
        thread.start()


def answer(conn, source):
    while True:
        data = conn.recv(1024)
        request = data.decode('utf-8')
        if request != '':
            if request.startswith('put') or request.startswith('get'):
                lst = request.split()
                print(lst)
                if lst[0] == 'put' and len(lst) == 4:
                    try:
                        lst[2] = float(lst[2])
                        lst[3] = int(lst[3])
                        conn.send(source.put(lst[1], lst[2], lst[3]).encode('utf-8'))
                    except:
                        conn.send(b'error\nwrong command\n\n')
                elif lst[0] == 'get' and len(lst) == 2:
                    conn.send(source.get(lst[1]).encode('utf-8'))
                else:
                    conn.send(b'error\nwrong command\n\n')
            else:
                conn.send(b'error\nwrong command\n\n')


if __name__ == '__main__':
    run_server('127.0.0.1', 10001)
