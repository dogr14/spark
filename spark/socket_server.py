import socket
import time

# 配置服务器的 IP 和端口
HOST = "0.0.0.0"  # 监听所有 IP 地址
PORT = 9999       # 监听端口

# 创建 Socket 服务端
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Listening on port {PORT}...")

# 等待客户端连接
client_socket, addr = server_socket.accept()
print(f"Connection from {addr} established!")

# 模拟持续发送数据
try:
    while True:
        message = f"test spark streaming data {time.time()}\n"
        client_socket.send(message.encode("utf-8"))
        print(f"Sent: {message.strip()}")
        time.sleep(2)  # 每隔 2 秒发送一条数据
except KeyboardInterrupt:
    print("Stopping server...")
finally:
    client_socket.close()
    server_socket.close()