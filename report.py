from process import direct_data,compete_data,history_data,combine_data,insert_data,today
from logger import logger
from send_mail import send_mail
from syspath import path
from pandas import concat
from os import chdir

chdir(path)

field_list = ["fetch_date","film_date","cinema_code","cinema_name","compete_cinema_code","compete_cinema_name","film_name","bo",\
              "people","session","occupancy","avg_price","cinema_session","session_percent","avg_session"]

field_dict = {"fetch_date":"数据获取日期","film_date":"票房日期","cinema_code":"影城编码","cinema_name":"影城","compete_cinema_code":"竞对影城编码",\
              "compete_cinema_name":"竞对影城","film_name":"影片","bo":"票房/元","people":"人数","session":"场次","occupancy":"上座率","avg_price":"平均票价",\
              "cinema_session":"影院总场次","session_percent":"影片场次占比","avg_session":"以往场次上限"}

if __name__ == "__main__":
    film_name = "八佰"
    df_table_list = direct_data(film_name)
    df_compete_list = compete_data()
    df_total = combine_data(df_table_list,df_compete_list,film_name)
    #先读取历史数据再写入
    df_history_data = history_data()
    insert_data(df_total)
    logger.info("processed data insert into database complete")
    if len(df_history_data) != 0:
        df_total = concat([df_history_data,df_total],ignore_index = True)
    df_total = df_total.reindex(columns = field_list)
    df_total.rename(columns = field_dict,inplace = True)
    #生成文件，发送邮箱
    file_name = "%s预售%s.xlsx" % (film_name,today)
    df_total.to_excel(file_name,sheet_name = film_name,header = True,index = False)
    send_mail(file_name)
    