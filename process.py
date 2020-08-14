import pandas as pd
import numpy as np
import datetime
from database import read_sql,to_sql,get_ftp
from logger import logger

today = datetime.date.today()

#原始数据处理
def data_filter(df,time1,time2):
    timelist = []
    showcount_time = df["场次时间"]
    for each_time in showcount_time:
        tmp_time = datetime.datetime.strptime(each_time,"%Y-%m-%d %H:%M:%S")
        delta1 = tmp_time - time1
        delta2 = time2 - tmp_time
        if delta1.days == 0 and delta2.days == 0:
            timelist.append(each_time)
    df = df[df["场次时间"].isin(timelist)]
    pat = "（.*?）\s*|\(.*?\)\s*|\s*"
    df["影片"].replace(pat,"",regex = True,inplace = True)
    df = df[df["场次状态"] == "开启"]
    df = df[df["是否最新"] == "是"]
    return df

#历史平均场次
def get_avg_session():
    df_avg_session = read_sql("select cinema_code,avg_session from avg_session")
    return df_avg_session

#日期规则
def date_rules():
    date_rules = read_sql("select film_date from date_rules where fetch_date = '%s'" % today)["film_date"].tolist()
    return date_rules

#历史数据
def history_data():
    df_history_data = read_sql("select film_date,fetch_date,cinema_code,cinema_name,compete_cinema_code,compete_cinema_name,film_name,bo,\
    people,session,occupancy,avg_price,cinema_session,session_percent,avg_session from film_session_result")
    return df_history_data
    
#写入已整理数据
def insert_data(df):
    test_df_total = read_sql("select * from film_session_result")
    test_df = read_sql("select * from film_session_result where fetch_date = '%s'" % today)
    #今天无数值或初始值为空
    if len(test_df) == 0 or len(test_df_total) == 0:
        to_sql(df,"film_session_result")

#直营数据处理
def direct_data(film_name):
    df_list = []
    df_table_list = []
    film_date_list = date_rules()
    #放到服务器后使用前一句
    #df = get_ftp(today)  
    df = pd.read_csv("C:\\Users\\Administrator\\Desktop\\SessionRevenue_20200811.csv",encoding = "utf-8")
    for each_date in film_date_list:
        t1 = datetime.datetime.strptime(str(each_date) + " 06:00:00","%Y-%m-%d %H:%M:%S")
        t2 = datetime.datetime.strptime(str(each_date + datetime.timedelta(days = 1)) + " 05:59:59","%Y-%m-%d %H:%M:%S")
        each_df = data_filter(df, t1, t2)
        df_list.append(each_df)
    
    for i in range(len(df_list)):
        #计算影片场次
        df_table = pd.pivot_table(df_list[i],index = ["影院","影片"],values = ["票房","上座率","人数","总座位数"],aggfunc = {"票房":np.sum,"上座率":len,\
                                                                                                              "人数":np.sum,"总座位数":np.sum},fill_value = 0,margins = False)
        df_table.reset_index(inplace = True)
        df_table = pd.DataFrame(df_table)
        #计算影院场次
        df_table_cinema = pd.pivot_table(df_list[i],index = ["影院"],values = ["影片"],aggfunc = {"影片":len},fill_value = 0,margins = False)
        df_table_cinema.reset_index(inplace = True)
        df_table_cinema = pd.DataFrame(df_table_cinema)
        df_table_cinema.rename(columns = {"影片":"影院总场次"},inplace = True)
        #筛选影片后匹配，计算指标
        df_table = df_table[df_table["影片"].isin([film_name])]
        df_table.rename(columns = {"上座率":"场次"},inplace = True)
        df_table_list.append([df_table,df_table_cinema])
    
    logger.info("direct data process complete")    
    return df_table_list

#竞对数据处理      
def compete_data():
    df_compete_list = []
    film_date_list = date_rules()
    for each_date in film_date_list:
        df_compete_cinema_film = read_sql("select compete_cinema_code,film_name,bo,session,people,occupancy \
        from compete_cinema_film where film_date = '%s' and fetch_date = '%s'" % (each_date,today))
        df_compete_list.append(df_compete_cinema_film)
    
    logger.info("compete data process complete")
    return df_compete_list

#合并拼接数据   
def combine_data(df_table_list,df_compete_list,film_name):
    df_cinema_total = pd.DataFrame(columns = ["cinema_code","cinema_name","compete_cinema_code","compete_cinema_name",\
                                              "film_name","bo","people","session","occupancy","cinema_session"])
    #直营影院列表
    df_jycinema = read_sql("select cinema_code,cinema_name,vista_cinema_name from jycinema_info where op_status = 1")
    df_compete_cinema = read_sql("select jycinema_code,jycinema_name,compete_cinema_code,compete_cinema_name from compete_cinema_info")
    jycinema_list = df_jycinema["cinema_code"].tolist()
    
    #利用film_date_list,df_table_list,df_compete_list对应性进行合并
    #注意，此时film_name只有1个，若为film_list则要稍微调整一下逻辑
    df_avg_session = get_avg_session()
    film_date_list = date_rules()
    for i in range(len(film_date_list)):
        df_jycinema_combine = pd.merge(left = df_jycinema,right = df_table_list[i][0],left_on = "vista_cinema_name",right_on = "影院",how = "left")
        df_jycinema_combine = pd.merge(left = df_jycinema_combine,right = df_table_list[i][1],left_on = "vista_cinema_name",right_on = "影院",how = "left")
        df_jycinema_combine["影片"] = film_name
        df_jycinema_combine["occupancy"] = np.round(df_jycinema_combine["人数"] / df_jycinema_combine["总座位数"],4) * 100
        df_jycinema_combine.drop(columns = ["影院_x","影院_y","总座位数","vista_cinema_name"],axis = 1,inplace = True)
        df_jycinema_combine.rename(columns = {"影片":"film_name","场次":"session","人数":"people","票房":"bo","影院总场次":"cinema_session"},\
                                   inplace = True)
        df_jycinema_combine = pd.merge(left = df_jycinema_combine,right = df_avg_session,on = "cinema_code",how = "left")
        
        df_compete_cinema_combine = pd.merge(left = df_compete_cinema,right = df_compete_list[i],on = "compete_cinema_code",how = "left")
        df_compete_cinema_combine["film_name"] = film_name
        
        #把竞对影院总场次匹配上
        df_compete_cinema_session = read_sql("select compete_cinema_code,session \
        from compete_cinema_session where film_date = '%s' and fetch_date = '%s'" % (film_date_list[i],today))
        df_compete_cinema_session.rename(columns = {"session":"cinema_session"},inplace = True)
        df_compete_cinema_combine = pd.merge(left = df_compete_cinema_combine,right = df_compete_cinema_session,\
                                             on = "compete_cinema_code",how = "left")
        df_compete_cinema_combine = pd.merge(left = df_compete_cinema_combine,right = df_avg_session,left_on = "compete_cinema_code",right_on = \
                                             "cinema_code",how = "left")
        df_compete_cinema_combine.drop(columns = ["cinema_code"],axis = 1,inplace = True)
        df_compete_cinema_combine.rename(columns = {"jycinema_code":"cinema_code","jycinema_name":"cinema_name"},inplace = True)

        #拼接自营及竞对
        for each_code in jycinema_list:
            each_df_jycinema = df_jycinema_combine[df_jycinema_combine["cinema_code"] == each_code]
            each_df_compete_cinema = df_compete_cinema_combine[df_compete_cinema_combine["cinema_code"] == each_code]
            each_cinema_total = pd.concat([each_df_jycinema,each_df_compete_cinema],ignore_index = True)
            df_cinema_total = pd.concat([df_cinema_total,each_cinema_total],ignore_index = True)
            
        df_cinema_total["fetch_date"] = today
        df_cinema_total["film_date"] = film_date_list[i]
    
    #处理字段
    for each_field in ["bo","people","session","cinema_session","occupancy"]:
        df_cinema_total[each_field].replace("",0,inplace = True)
    df_cinema_total.fillna(0,inplace = True)
    df_cinema_total["compete_cinema_code"].replace(0,"",inplace = True)
    df_cinema_total["compete_cinema_name"].replace(0,"",inplace = True)
    df_cinema_total["cinema_session"] = df_cinema_total["cinema_session"].astype(int)
    df_cinema_total["occupancy"] = df_cinema_total["occupancy"].astype(float)
    #计算两列
    df_cinema_total["avg_price"] = np.round(np.divide(df_cinema_total["bo"],df_cinema_total["people"],out = np.zeros_like(df_cinema_total["bo"]),\
                                                                                                                 where = df_cinema_total["people"] != 0),2)
    df_cinema_total["session_percent"] = np.round(np.divide(df_cinema_total["session"],df_cinema_total["cinema_session"],\
                                                            out = np.zeros_like(df_cinema_total["session"]),where = df_cinema_total["cinema_session"] != 0),4) * 100
    
    logger.info("combine data process complete")
    return df_cinema_total
#     df_cinema_total = df_cinema_total.reindex(columns = field_list)




