import logging
import datetime
from process import today

def get_logger():
    today = str(datetime.date.today())
    #创建logger
    logger = logging.getLogger(__name__)
    logger.setLevel(level = logging.INFO)
    #handler用于创建log文件
    handler = logging.FileHandler("C:\\Users\\Administrator\\Desktop\\film_report_log_%s.txt" % today)
    handler.setLevel(level = logging.INFO)
    #formatter用于设置log日志格式
    formatter = logging.Formatter("%(asctime)s  %(name)s  %(levelname)s  %(message)s",datefmt = "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

#定义变量用于调用
logger = get_logger()