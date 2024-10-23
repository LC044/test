import logging
import os
import time
import traceback
from functools import wraps
filename = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(time.time()))
logger = logging.getLogger('test')
logger.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
try:
    if not os.path.exists('./log/logs'):
        os.mkdir('./log/logs')
    # file_handler = logging.FileHandler(f'./log/logs/{filename}-log.log')
    file_handler = logging.handlers.RotatingFileHandler(f'./log/logs/{filename}-log.log', maxBytes=5*1024*1024, backupCount=3)
    file_handler2 = logging.FileHandler(f'./log/logs/{filename}-log0.log')
except:
    file_handler = logging.FileHandler(f'日志文件-{filename}-log.log')
    file_handler2 = logging.FileHandler(f'日志文件-{filename}-log0.log')

file_handler.setLevel(level=logging.DEBUG)
file_handler.setFormatter(formatter)
file_handler2.setLevel(level=logging.ERROR)
file_handler2.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(file_handler2)
logger.addHandler(stream_handler)


def log(func):
    @wraps(func)
    def log_(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"\n{func.__qualname__} is error,params:{(args, kwargs)},here are details:\n{traceback.format_exc()}")
    return log_