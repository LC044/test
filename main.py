import multiprocessing
import socket
import traceback
import time

from build_index import Phase1, Phase2, Phase4, init, Phase3
from database.opengauss import OpenGauss
from log.logger import logger

dbname = 'test1'

def phase1_task():
    phase1 = Phase1(dbname)
    phase1.run()

def phase2_task():
    logger.error('阶段二开始')
    phase2 = Phase2(dbname)
    phase2.run()
    logger.error('阶段二结束')

def phase3_task():
    logger.error('阶段三开始')
    phase3 = Phase3(dbname)
    phase3.run()
    logger.error('阶段三结束')

def phase4_task():
    phase4 = Phase4(dbname)
    phase4.run()

def insert_data():
    og = OpenGauss(dbname)
    og.roll_back_rate = -0.1
    og.init_database()
    # og.random_operation()
    # for i in range(5000):
    #     og.insert_one()
    # og.insert_many_rows(10000)
    # og.print()
    og.close()

def main(db_name):
    global dbname
    dbname = db_name
    try:
        st_time = time.time()
        logger.error(f'start test,time:{st_time}')
        # 1. 初始化数据库
        init(dbname)

        # 2. 插入一些数据
        insert_data()

        # 启动TCP服务器
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('127.0.0.1', 8080))
        server_socket.listen(1)
        print("Server is listening on port 8080...")

        # 3. 创建进程执行在线创建索引任务
        task1_process = multiprocessing.Process(target=phase1_task)
        task1_process.start()
        time.sleep(1)
    
        # 4. 等待客户端连接（openGauss进入阶段2）
        conn, addr = server_socket.accept()
        print(f"Connection from {addr} established.")

        # 5. 执行阶段2的DML操作
        phase2_task()

        # 6. 通知openGauss进入阶段3
        conn.send(b"phase 2 completed, start phase3")
        data = conn.recv(1024)
        print(f"C++ task completed signal received. {data} ")

        # 7. 执行阶段3的DML操作
        phase3_task()
        conn.send(b"phase 3 completed, start phase4")
        print('phase 3 completed, start phase4')
        # 8. 等待openGauss索引创建完成
        data = conn.recv(1024)
        print(f"C++ task completed signal received. {data} ")
        print(f"index build completed. receive")

        # 9. 执行任务4，验证索引的正确性
        phase4_task()
    finally:
        en_time = time.time()
        logger.error(f'end test,time:{en_time},cost:{en_time-st_time}s')
        # logger.error(traceback.format_exc())
        server_socket.close()

if __name__ == '__main__':
    n = 1 # 测试个数
    for i in range(n):
        logger.error(f'第{i+1}次测试')
        main(f'test{i+1}')