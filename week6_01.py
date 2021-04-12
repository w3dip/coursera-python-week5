import asyncio
import threading

mutex = threading.RLock()
storage = {}

class ClientServerProtocol(asyncio.Protocol):

    def get(self, key):
        with mutex:
            result = ["ok"]
            if key == "*":
                for data_array_key in storage:
                    for data_item in storage.get(data_array_key):
                        result.append(f"{data_array_key} {data_item[0]} {data_item[1]}")
            else:
                data_array = storage.get(key)
                if data_array:
                    for data_item in data_array:
                        result.append(f"{key} {data_item[0]} {data_item[1]}")
            result = "\n".join(result)
            return f"{result}\n\n"

    def put(self, key, value, timestamp):
        with mutex:
            value_arr = storage.get(key)
            if not value_arr:
                storage[key] = [(value, timestamp)]
            else:
                was_replace = False
                for index, value_item in enumerate(value_arr):
                    if value_item[1] == timestamp:
                        value_arr[index] = (value, timestamp)
                        was_replace = True

                if not was_replace:
                    value_arr.append((value, timestamp))
                storage[key] = value_arr
            return "ok\n\n"

    def process_data(self, data):
        if not data:
            return self.wrong_command
        data_array = data.split()

        if len(data_array) == 0 or len(data_array) < 2:
            return self.wrong_command

        command = data_array[0]
        key = data_array[1]
        if command == "get":
            if len(data_array) > 2:
                return self.wrong_command
            return self.get(key)
        if command == "put":
            if len(data_array) > 4:
                return self.wrong_command
            try:
                value = float(data_array[2])
                timestamp = int(data_array[3])
                return self.put(key, value, timestamp)
            except ValueError:
                return self.wrong_command

        return self.wrong_command

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())

    def __init__(self):
        self.wrong_command = "error\nwrong command\n\n"

def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

#run_server("127.0.0.1", 8888)