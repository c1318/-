from sqlalchemy import create_engine
import pymysql
import pandas as pd
from ftplib import FTP
from re import match

def to_sql(df,tablename):
    conn = create_engine("mysql+pymysql://root:123456@localhost/test?charset=utf8")
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)
    
def read_sql(sql):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "123456",db = "test",charset = "utf8")
    res = pd.read_sql(sql,conn)
    return pd.DataFrame(res)

def get_ftp(date):
    ftp = FTP()
    ftp.connect(host = "172.20.240.195",port = 21,timeout = 30)
    ftp.login(user = "sjzx",passwd = "jy123456@")
    date = date.replace("-","")
    filename = "SessionRevenue_" + date + ".csv"
    for each_file in list:
        judge = match(filename,each_file)
        if judge:
            file_handle = open(filename,"wb+")
            ftp.retrbinary("RETR " + filename,file_handle.write)
            file_handle.close()
            
    ftp.quit()
    return filename
    