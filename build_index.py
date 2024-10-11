import traceback
import psycopg2
from opengauss import OpenGauss
import multiprocessing
import random
from logger import logger

class Phase1(OpenGauss):
    def __init__(self, dbname):
        super().__init__(dbname)

    def create_index(self):
        sql = '''CREATE INDEX concurrently idx_users_name_email ON users(name,email);'''
        self.cursor.execute(sql)
        self.connection.commit()

    def run(self):
        self.create_index()
        self.close()

class Phase2(OpenGauss):
    def __init__(self, dbname):
        super().__init__(dbname)

    def run(self):
        num_processes = 5
        processes = []
        for _ in range(num_processes):
            p = multiprocessing.Process(target=self.random_operation)
            processes.append(p)
            p.start()

        # 等待所有进程完成
        for p in processes:
            p.join()