import os
import pandas as pd
from datetime import datetime
import shutil
import glob

from helper import postgresql_helper as pg
# from helper import command_helper
import config
import core.orderbook as ob
import core.executed as ex

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
    filter_dest_path = config.PATH_CONFIG['filter_dest_path']

    filter_dest_dir = filter_dest_path + episode_type
    if os.path.exists(filter_dest_dir) is False:
        os.makedirs(filter_dest_dir)

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
            dest_file_path = filter_dest_dir + "\\" + dest_file_name

            shutil.copy2(file, dest_file_path)
            file_count = file_count + 1
            print(str(file_count) + ' copy... :' + file)


    return file_count, filter_dest_dir


def create_scaling_episode_file(filter_dest_dir, episode_type):
    file_list = os.listdir(filter_dest_dir)
    int_sec = 1;
    scaling_dest_path = config.PATH_CONFIG['scaling_dest_path']+episode_type+"\\"

    filter_scaling_mapping_file_path = scaling_dest_path + "mapping\\";
    if os.path.exists(filter_scaling_mapping_file_path) is False:
        os.makedirs(filter_scaling_mapping_file_path)

    file_count = 0
    mapping_df = pd.DataFrame(columns=['filter', 'scaling'])
    for file in file_list:
        file_split = file.split("_")
        type_str = file_split[0]
        date_str = file_split[1].replace("-","")
        code_str = file_split[3]
        scaling_file_name = type_str + "-" + code_str + "-" + date_str
        path_src = filter_dest_dir + "\\" + file;
        df = pd.DataFrame()
        if file.find("executed") is not -1:
            scaling_file_name += "-quote.csv"
            machine = ob.OB_Preprocess()
            df = machine.preprocess(path_src, int_sec)
            df = machine.scail(df)
            df.to_csv(scaling_dest_path + scaling_file_name , index=False)
            file_count = file_count + 1
            print(str(file_count) + ' create... :' + scaling_dest_path + scaling_file_name)
        elif file.find("orderbook") is not -1:
            scaling_file_name += "-order.csv"
            machine = ex.EX_Preprocess()
            df = machine.preprocess(path_src, int_sec)
            df = machine.scail(df)
            df.to_csv(scaling_dest_path + scaling_file_name, index=False)
            file_count = file_count + 1
            print(str(file_count) + ' create... :' + scaling_dest_path + scaling_file_name)

        mapping_df = mapping_df.append({'filter':file, 'scaling':scaling_file_name},ignore_index=True)
    mapping_df.to_csv(filter_scaling_mapping_file_path + "mapping.csv", index=False)
    return scaling_dest_path, file_count





def main(start_date, end_date, min_rate, max_rate, episode_type):
    # df = get_df_toDb(start_date, end_date, min_rate, max_rate)
    df = pd.read_csv(".\\output\\test\\selectMaxTicker2.csv", dtype={'code': str, 'date': 'str', "type": str})
    df = df.fillna("")
    # df.to_csv("output\\test\\selectMaxTicker2.csv", index=False)
    print('sql result : ' + str(len(df)))

    file_count, filter_dest_dir = create_episode_file(df, episode_type)

    print("Episode[" + filter_dest_dir + "], Files["+str(file_count) + "] created.")

    scaling_dest_path, file_count = create_scaling_episode_file(filter_dest_dir, episode_type)
    print("Episode[" + scaling_dest_path + "], Files["+str(file_count) + "] created.")

if __name__ == "__main__":
    main('20180510','20180510','25','30', '01')

    # main(command_helper.start_date, command_helper.end_date, command_helper.min_rate, command_helper.max_rate, command_helper.episode_type)