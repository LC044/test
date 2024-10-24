import traceback
import psycopg2
from database.opengauss import OpenGauss,connection_params
import multiprocessing
import random
from log.logger import logger

class Phase1(OpenGauss):
    """
    创建在线索引
    """
    def __init__(self, dbname):
        super().__init__(dbname)
        self.connection.autocommit = True

    def create_index(self):
        logger.error(f'{self.__class__.__name__} start build  index concurrently')
        sql = '''CREATE INDEX concurrently idx_users_name_email ON users(name,email);'''
        # sql = '''CREATE unique INDEX concurrently idx_users_age ON users(id);'''
        self.cursor.execute(sql)
        self.connection.commit()
        logger.error(f'{self.__class__.__name__} end build index concurrently')

    def run(self):
        self.create_index()
        self.close()

class Phase2:
    def __init__(self, dbname):
        self.dbname = dbname
        
    def task(self):
        og = OpenGauss(self.dbname)
        og.random_operation(500,op_rate=[3,0,3])
        og.close()
    
    def run(self):
        num_processes = 5
        processes = []
        for _ in range(num_processes):
            task1_process = multiprocessing.Process(target=self.task)
            task1_process.start()
            processes.append(task1_process)
        for process in processes:
            process.join()

class Phase3:
    def __init__(self, dbname):
        self.dbname = dbname
        
    def task(self):
        og = OpenGauss(self.dbname)
        og.random_operation(500,op_rate=[0,3,3])
        og.close()
    
    def run(self):
        num_processes = 5
        processes = []
        for _ in range(num_processes):
            task1_process = multiprocessing.Process(target=self.task)
            task1_process.start()
            processes.append(task1_process)
        for process in processes:
            process.join()

class Phase4(OpenGauss):
    """
    对比索引和表数据的差异
    """
    def __init__(self, dbname):
        super().__init__(dbname)
    
    def get_table_data(self,table,columns):
        column_str = ', '.join(columns)
        query = f"SELECT {column_str} FROM {table};"
        self.cursor.execute(query)
        table_data = self.cursor.fetchall()
        self.connection.commit()
        return table_data

    def get_index_data(self, table, columns, index=None):
        # 强制使用索引的查询
        column_str = ', '.join(columns)
        query = f"SET enable_seqscan = OFF;SET enable_bitmapscan=OFF; SELECT {column_str} FROM {table} WHERE ({column_str}) IS NOT NULL;"
        self.cursor.execute(query)
        index_data = self.cursor.fetchall()
        self.connection.commit()
        return index_data

    def compare_data(self, table_data, index_data):
        table_data_set = set(table_data)
        index_data_set = set(index_data)

        if table_data_set == index_data_set:
            print("The index corresponds correctly with the table data.")
            logger.error("The index corresponds correctly with the table data.")
        else:
            print("The index does NOT correspond with the table data.")
            logger.error("The index corresponds correctly with the table data.")
            # 输出差异
            missing_in_index = table_data_set - index_data_set
            missing_in_table = index_data_set - table_data_set

            if missing_in_index:
                print("Data in table but missing in index:", missing_in_index)
                logger.error(f"Data in table but missing in index:{missing_in_index}")
            if missing_in_table:
                print("Data in index but missing in table:", missing_in_table)
                logger.error(f"Data in index but missing in table:{missing_in_table}")
    
    def run(self):
        table = 'users'
        columns = ['name', 'email']
        # columns = ['id']
         # 获取表中的多列组合数据
        table_data = self.get_table_data(table, columns)
        print(f"Table data count: {len(table_data)}")
        logger.error(f"Table data count: {len(table_data)}")
        # 获取索引中的多列组合数据
        index_data = self.get_index_data(table, columns)
        print(f"Index data count: {len(index_data)}")
        logger.error(f"Index data count: {len(index_data)}")
        # 比较表数据和索引数据
        self.compare_data(table_data, index_data)
        self.close()

def init(dbname):
    # 初始化数据库
    try:
        # 连接数据库
        connection = psycopg2.connect(**connection_params)
        connection.autocommit = True
        cursor = connection.cursor()

        # 插入数据（在事务中）
        insert_query = f"""DROP DATABASE IF EXISTS {dbname};"""
        cursor.execute(insert_query)
        print(f'删除数据库 {dbname}')
        connection.commit()
        # 插入数据（在事务中）
        insert_query = f"""CREATE DATABASE {dbname};"""
        cursor.execute(insert_query)
        print(f'创建数据库 {dbname}')
        connection.commit()

    except psycopg2.Error as e:
        print(f"数据库错误: {e}")
        # 出错时回滚
        connection.rollback()

    finally:
        # 关闭游标和数据库连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == '__main__':
    db_name = 'test1'
    # init(db_name)
    # og = OpenGauss(db_name)
    # og.init_database()
    # og.random_operation()
    # og.insert_one()
    # og.insert_many_rows(1000)
    # while(not og.insert_one()):
    #     logger.info('没有回滚')
    # # og.print()
    # og.close()
    # phase1 = Phase1(db_name)
    # phase1.run()
    # phase2 = Phase3('test1')
    # phase2.run()
    phase4 = Phase4(db_name)
    phase4.run()

    # phase2 = Phase2('test1')
    # phase2.run()