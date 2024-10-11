import psycopg2
from opengauss import OpenGauss
from logger import logger

def init():
    # 初始化数据库
    connection_params = {
        'host': '127.0.0.1',
        'port': '33000',
        'dbname': 'postgres',
        'user': 'shuaikangzhou',
        'password': 'zhou@123'
    }

    try:
        # 连接数据库
        connection = psycopg2.connect(**connection_params)
        connection.autocommit = True
        cursor = connection.cursor()

        # 插入数据（在事务中）
        insert_query = """
        DROP DATABASE IF EXISTS test1;
        """
        cursor.execute(insert_query)
        logger.info('删除数据库 test1')
        connection.commit()
        # 插入数据（在事务中）
        insert_query = """
        CREATE DATABASE test1;
        """
        cursor.execute(insert_query)
        logger.info('创建数据库 test1')
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


# init()
og = OpenGauss('test1')
# og.init_database()
og.random_operation()
# og.insert_one()
# og.insert_many_rows(5)
og.print()

