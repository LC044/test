import psycopg2
from faker import Faker
fake = Faker()

class OpenGauss:
    def __init__(self,dbname):
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
        );
        '''
        self.cursor.execute(sql)
        self.connection.commit()

    def insert_one_row(self):
        sql = '''INSERT INTO users (name, age, email) VALUES(%s,%s,%s);'''
        # 生成随机数据
        random_name = fake.name()
        random_age = fake.random_int(min=18, max=80)
        random_email = fake.email()
        self.cursor.execute(sql,[random_name,random_age,random_email])
        self.connection.commit()

    def insert_many_rows(self,n):
        sql = '''INSERT INTO users (name, age, email) VALUES(%s,%s,%s);'''
        for _ in range(n):
            # 生成随机数据
            random_name = fake.name()
            random_age = fake.random_int(min=18, max=80)
            random_email = fake.email()
            self.cursor.execute(sql,[random_name,random_age,random_email])
        self.connection.commit()

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