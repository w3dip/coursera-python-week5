import socket
import datetime

class ClientError(Exception):

    def __init__(self, message):
        self.message = message

class Client:

    def __init__(self, host, port, timeout=None):
        self.sock = socket.socket()
        self.sock.connect((host, port))
        self.sock.settimeout(timeout)
        self.charset = "utf8"

    def __del__(self):
        self.sock.close()

    def get(self, metric):
        result = {}
        if not metric:
            return result
        try:
            self.sock.sendall(f"get {metric}\n".encode(self.charset))
            data = self.sock.recv(1024)
            if not data:
                raise ClientError("No data")
            data_str = data.decode(self.charset)
            if data_str.startswith("error"):
                raise ClientError("Server error")
            data_array = data_str.split('\n')
            if len(data_array) < 2:
                return result
            else:
                if data_array[0] != "ok":
                    raise ClientError("Server error")
                for data_item in data_array[1:-2]:
                    data_item_array = data_item.split()
                    if len(data_item_array) < 3:
                        raise ClientError("Server error")
                    key = data_item_array[0]
                    try:
                        value = float(data_item_array[1])
                        time = int(data_item_array[2])
                        value_arr = result.get(key)
                        if not value_arr:
                            result[key] = [(time, value)]
                        else:
                            value_arr.append((time, value))
                            value_arr.sort(key=lambda tup: tup[0])
                            result[key] = value_arr
                    except ValueError:
                        raise ClientError("Server error")
                return dict(sorted(result.items(), key=lambda item: item[1]))
        except socket.timeout:
            raise ClientError("Timeout error")
        except socket.error as ex:
            raise ClientError("Server error")

    def put(self, metric, value, timestamp=None):
        try:
            if not timestamp:
                timestamp = int(datetime.datetime.now().timestamp())
            self.sock.sendall(f"put {metric} {value} {timestamp}\n".encode(self.charset))
            data = self.sock.recv(1024)
            if not data:
                raise ClientError("No data")
            data_str = data.decode(self.charset)
            if data_str.startswith("error"):
                raise ClientError("Server error")
        except socket.timeout:
            raise ClientError("Timeout error")
        except socket.error as ex:
            raise ClientError("Server error")

#client = Client("127.0.0.1", 8888, timeout=15)
#print(client.get("*"))
#client.put("palm.cpu", 0.5, timestamp=1150864247)