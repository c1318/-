import pandas as pd
from database import to_sql,read_sql
from os import chdir
from syspath import path
from logger import logger

chdir(path)

df1 = pd.read_excel("import.xlsx",sheet_name = "影院影片数据")
df2 = pd.read_excel("import.xlsx",sheet_name = "影院总场次")
film_field_dict = {"影院编码":"compete_cinema_code","影院名称":"compete_cinema_name","影片":"film_name","票房(万)":"bo","总场次":"session",\
                   "总人次":"people","平均票价":"avg_price","上座率":"occupancy","票房日期":"film_date","获取日期":"fetch_date"}
cinema_field_dict = {"影院编码":"compete_cinema_code","影院名称":"compete_cinema_name","排场数":"session","票房日期":"film_date","获取日期":"fetch_date"}

#对数据进行筛选，适当处理后导入库里
def df_filter(df,field_dict,table_name):
    compete_cinema_list = read_sql("select compete_cinema_code from compete_cinema_info")["compete_cinema_code"].tolist()
    df = df[df["影院编码"].isin(compete_cinema_list)]
    df = df[list(field_dict.keys())]
    df.rename(columns = field_dict,inplace = True)
    if table_name == "compete_cinema_film":
        df["avg_price"].replace({"-":0.00},inplace = True)
        df["occupancy"].replace({"-":0.00},inplace = True)
        df["bo"] = df["bo"] *10000
        df["occupancy"] = df["occupancy"] * 100
    df["film_date"] = pd.to_datetime(df["film_date"])
    df["film_date"] = df["film_date"].apply(lambda x : x.strftime("%Y-%m-%d"))
    df["fetch_date"] = pd.to_datetime(df["fetch_date"])
    df["fetch_date"] = df["fetch_date"].apply(lambda x : x.strftime("%Y-%m-%d"))
    to_sql(df,table_name)
    logger.info("import data successfully inserted into database")

def fetch_data():
    df_filter(df1, film_field_dict, "compete_cinema_film")
    df_filter(df2,cinema_field_dict,"compete_cinema_session")
    