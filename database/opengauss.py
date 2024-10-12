import traceback
import psycopg2
import random
from faker import Faker
from log.logger import logger

fake = Faker()

# 定义装饰器，处理异常并回滚事务
def rollback_on_failure(func):
    def wrapper(self, *args, **kwargs):
        try:
            # 调用实际的函数
            result = func(self, *args, **kwargs)
            logger.info(f'{func.__name__}')
            return result
        except Exception as e:
            # 如果发生错误，则回滚事务
            logger.info(f"Error occurred: {e}. Rolling back the transaction.")
            self.connection.rollback()  # 回滚事务
        finally:
            # 可选地关闭游标或其他资源清理
            pass
    return wrapper


class OpenGauss:
    def __init__(self, dbname):
        self.dbname = dbname
        # 配置数据库连接信息
        connection_params = {
            'host': '127.0.0.1',
            'port': '33000',
            'dbname': dbname,
            'user': 'shuaikangzhou',
            'password': 'zhou@123'
        }
        self.connection = psycopg2.connect(**connection_params)
        self.cursor = self.connection.cursor()

    def init_database(self):
        sql = '''
        -- 删除表如果它存在
        DROP TABLE IF EXISTS users;

        -- 创建表，并包含主键和其他字段
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,  -- 主键字段，使用 SERIAL 类型自动生成唯一ID
            name VARCHAR(100),      -- 示例字段1：名字
            age INT,                -- 示例字段2：年龄
            email VARCHAR(255)      -- 示例字段3：电子邮件
        ) with (storage_type=ustore);
        '''
        self.cursor.execute(sql)
        self.connection.commit()

    @rollback_on_failure
    def insert_one(self):
        sql = '''INSERT INTO users (name, age, email) VALUES(%s,%s,%s);'''
        # 生成随机数据
        random_name = fake.name()
        random_age = fake.random_int(min=18, max=80)
        random_email = fake.email()
        self.cursor.execute(sql, [random_name, random_age, random_email])
        self.connection.commit()

    @rollback_on_failure
    def delete_one(self):
        # 随机删除一条记录
        query = "DELETE FROM users WHERE id = (SELECT id FROM users ORDER BY RANDOM() LIMIT 1)"
        self.cursor.execute(query)
        self.connection.commit()

    @rollback_on_failure
    def update_one(self):
        choice = random.random()
        if choice < 0.33:
            new_data = fake.random_int(min=18, max=80)
            query = "UPDATE users SET age = %s WHERE id = (SELECT id FROM users ORDER BY RANDOM() LIMIT 1)"
        elif choice < 0.66:
            new_data = fake.name()
            query = "UPDATE users SET name = %s WHERE id = (SELECT id FROM users ORDER BY RANDOM() LIMIT 1)"
        else:
            new_data = fake.email()
            query = "UPDATE users SET email = %s WHERE id = (SELECT id FROM users ORDER BY RANDOM() LIMIT 1)"
        self.cursor.execute(query, (new_data,))
        self.connection.commit()

    @rollback_on_failure
    def insert_many_rows(self, n):
        sql = '''INSERT INTO users (name, age, email) VALUES(%s,%s,%s);'''
        for _ in range(n):
            # 生成随机数据
            random_name = fake.name()
            random_age = fake.random_int(min=18, max=80)
            random_email = fake.email()
            self.cursor.execute(sql, [random_name, random_age, random_email])
        self.connection.commit()

    @rollback_on_failure
    def random_operation(self):
        op_num = random.randint(5, 10)
        for i in range(op_num):
            try:
                # 随机选择操作
                operations = [self.insert_one, self.delete_one, self.update_one]
                operation = random.choice(operations)
                operation()
            except:
                logger.error(traceback.format_exc())

    def print(self):
        sql = '''select * from users;'''
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        if results:
            for row in results:
                print(row)

    def close(self):
        # 关闭游标和数据库连接
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
