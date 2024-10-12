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
        self.close()



class Phase4(OpenGauss):
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
        query = f"SET enable_seqscan = OFF; SELECT {column_str} FROM {table} WHERE ({column_str}) IS NOT NULL;"
        self.cursor.execute(query)
        index_data = self.cursor.fetchall()
        self.connection.commit()
        return index_data

    def compare_data(self, table_data, index_data):
        table_data_set = set(table_data)
        index_data_set = set(index_data)

        if table_data_set == index_data_set:
            print("The index corresponds correctly with the table data.")
        else:
            print("The index does NOT correspond with the table data.")
            # 输出差异
            missing_in_index = table_data_set - index_data_set
            missing_in_table = index_data_set - table_data_set

            if missing_in_index:
                print("Data in table but missing in index:", missing_in_index)
            if missing_in_table:
                print("Data in index but missing in table:", missing_in_table)
    
    def run(self):
        table = 'users1'
        columns = ['name', 'email']
         # 获取表中的多列组合数据
        table_data = self.get_table_data(table, columns)
        print(f"Table data count: {len(table_data)}")

        # 获取索引中的多列组合数据
        index_data = self.get_index_data(table, columns)
        print(f"Index data count: {len(index_data)}")

        # 比较表数据和索引数据
        self.compare_data(table_data, index_data)

        self.close()

if __name__ == '__main__':
    phase4 = Phase4('test1')
    phase4.run()