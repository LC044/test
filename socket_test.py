# server.py
import socket

def main():
    # 创建TCP套接字
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 绑定服务器地址和端口
    server_socket.bind(('127.0.0.1', 8080))
    
    # 监听连接
    server_socket.listen(1)
    print("Server is listening on port 8080...")

    # 等待客户端连接
    conn, addr = server_socket.accept()
    print(f"Connection from {addr} has been established!")

    # 接收来自客户端的消息
    data = conn.recv(1024).decode('utf-8')
    print(f"Message from client: {data}")

    # 向客户端发送响应
    message = "Hello from Python server"
    conn.send(message.encode('utf-8'))
    
    # 关闭连接
    conn.close()
    server_socket.close()

if __name__ == "__main__":
    main()
