# server.py
import socket
import multiprocessing
import time

# 定义任务
def task_1():
    print("index build started...")
    time.sleep(5)  # 模拟耗时任务
    

def task_2():
    print("phase 2 DML started...")
    time.sleep(5)  # 模拟耗时任务
    print("phase 2 DML completed.")

def task_3():
    print("phase 3 DML started...")
    time.sleep(5)  # 模拟耗时任务
    print("phase 3 DML completed.")

def task_4():
    print("check index started...")
    time.sleep(5)  # 模拟耗时任务
    print("check index completed.")

def tcp_server():
    # 启动TCP服务器
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 8080))
    server_socket.listen(1)
    print("Server is listening on port 8080...")

    # 等待客户端连接
    conn, addr = server_socket.accept()
    print(f"Connection from {addr} established.")

    # 创建进程执行任务2
    task2_process = multiprocessing.Process(target=task_2)
    task2_process.start()
    task2_process.join()  # 等待任务2完成

    # 通知客户端可以开始执行任务
    conn.send(b"Task 2 completed, start C++ task")

    # 创建进程执行任务3
    task3_process = multiprocessing.Process(target=task_3)
    task3_process.start()
    task3_process.join()

    # 等待客户端完成任务
    data = conn.recv(1024)
    print("index build completed.")

    if data == b"C++ task completed":
        print("C++ task completed signal received.")
        # 创建进程执行任务4
        task4_process = multiprocessing.Process(target=task_4)
        task4_process.start()
        task4_process.join()

    # 关闭连接
    conn.close()
    server_socket.close()

if __name__ == "__main__":
    # 创建进程执行任务1
    task1_process = multiprocessing.Process(target=task_1)
    task1_process.start()
    # task1_process.join()  # 等待任务1完成

    # 启动TCP服务器
    tcp_server()
