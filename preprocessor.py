import sys
import os
import pandas as pd
from datetime import datetime,timedelta
import re
import shutil
import glob


import postgresql_helper as pg
import command_helper
import config

#os.environ['HTTP_PROXY']='http://70.10.15.10:8080'
#os.environ['HTTPS_PROXY']='http://70.10.15.10:8080'

def get_df_toDb(start_date, end_date, min_rate, max_rate):
    sql = (
        "SELECT O.*, S.NAME, S.TYPE \n" 
        "  FROM ( \n"
        "       select t1.code, t1.date, t1.open, t1.high, t1.low,t1.close,t1.volume \n"  
        "              ,(t1.close-t2.close) diff, t2.close last_close \n" 
        "              ,(t1.close-t2.close)/t2.close*100 as per \n" 
        "         from (SELECT *, ROW_NUMBER() OVER (ORDER BY code, date ASC) as no FROM ohlc) t1, \n" 
        "              (SELECT *, ROW_NUMBER() OVER (ORDER BY code, date ASC) as no FROM ohlc) t2 \n" 
        "        where t1.code = t2.code \n" 
        "          and t1.no = t2.no+1 \n" 
        "       ) O \n" 
        "  LEFT OUTER JOIN \n" 
        "       STOCK_COMPANY S \n" 
        "    ON O.CODE = S.CODE \n" 
        " WHERE TO_DATE(DATE, 'YYYYMMDD') >= TO_DATE('"+ start_date+"', 'YYYYMMDD') \n" 
        "   AND TO_DATE(DATE, 'YYYYMMDD') <= TO_DATE('"+ end_date + "', 'YYYYMMDD') \n" 
        "   AND O.PER >= " + min_rate + "\n"
        "   AND O.PER <= " + max_rate + "\n"
        " ORDER BY CODE, DATE \n")

    db = pg.PostgreDB()
    conn = db.get_conn()
    result = pd.read_sql(sql, conn)
    return result

def create_episode_file(df, episode_type):
    src_path = config.PATH_CONFIG['src_path']
    dest_path = config.PATH_CONFIG['dest_path']

    dest_dir = dest_path + episode_type
    if os.path.exists(dest_dir) is False:
        os.makedirs(dest_dir)

    file_list = os.listdir(src_path)
    print('source file list:' + str(len(file_list)))
    file_count = 0

    for index, row in df.iterrows():
        code = row["code"]
        date_str = row["date"]
        date = datetime.strptime(date_str, "%Y%m%d")
        convert_date = date.strftime('%Y-%m-%d')
        file_name = convert_date + '_*_' + code + '_*.csv';

        for file in glob.glob(src_path + file_name):
            file_split = file.split("\\")
            src_file_name = file_split[len(file_split) - 1]
            dest_file_name = episode_type + "_" + src_file_name
            dest_file_path = dest_dir + "\\" + dest_file_name

            shutil.copy2(file, dest_file_path)
            file_count = file_count + 1
            print(str(file_count) + ' copy... :' + file)

    return file_count, dest_dir

def main(start_date, end_date, min_rate, max_rate, episode_type):
    df = get_df_toDb(start_date, end_date, min_rate, max_rate)
    #df = pd.read_csv(".\\output\\test\\selectMaxTicker.csv", dtype={'code': str, 'date': 'str', "type": str})
    df = df.fillna("")

    print('sql result : ' + str(len(df)))

    file_count, dest_dir = create_episode_file(df, episode_type)

    print("Episode[" + dest_dir + "], Files["+str(file_count) + "] created.")

if __name__ == "__main__":
    #main('','','','', '02')

    main(command_helper.start_date, command_helper.end_date, command_helper.min_rate, command_helper.max_rate, command_helper.episode_type)