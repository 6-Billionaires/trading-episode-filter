import os
import pandas as pd
from datetime import datetime
import shutil
import glob

from helper import postgresql_helper as pg
import config
import core.orderbook as ob
import core.executed as ex

class Episod_filter():

    def __init__(self):
        self.path_seperator = "/"
        self.int_sec = 1
        self.src_path = config.PATH_CONFIG['src_path']
        self.filter_dest_path = config.PATH_CONFIG['filter_dest_path']
        self.scaling_dest_path = config.PATH_CONFIG['scaling_dest_path']

    def setup(self, path_seperator, src_path, filter_dest_path, scaling_dest_path):
        self.path_seperator = path_seperator
        self.src_path = src_path
        self.filter_dest_path = filter_dest_path
        self.scaling_dest_path = scaling_dest_path

    def get_df_toDb(self, start_date, end_date, min_rate, max_rate):
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

    def create_episode_file(self, df, episode_type):

        filter_dest_dir = self.filter_dest_path + episode_type
        if os.path.exists(filter_dest_dir) is False:
            os.makedirs(filter_dest_dir)

        file_list = os.listdir(self.src_path)
        print('source file list:' + str(len(file_list)))
        file_count = 0

        for index, row in df.iterrows():
            code = row["code"]
            date_str = row["date"]
            date = datetime.strptime(date_str, "%Y%m%d")
            convert_date = date.strftime('%Y-%m-%d')
            file_name = convert_date + '_*_' + code + '_*.csv';

            for file in glob.glob(self.src_path + file_name):
                file_split = file.replace("\\", self.path_seperator).split(self.path_seperator)
                src_file_name = file_split[len(file_split) - 1]
                dest_file_name = episode_type + "_" + src_file_name
                dest_file_path = filter_dest_dir + self.path_seperator + dest_file_name
                shutil.copy2(file, dest_file_path)
                file_count = file_count + 1
                print(str(file_count) + ' copy... :' + file)


        return file_count, filter_dest_dir


    def create_scaling_episode_file(self, filter_dest_dir, episode_type):
        file_list = os.listdir(filter_dest_dir)

        scaling_dest_dir = self.scaling_dest_path + episode_type + self.path_seperator
        filter_scaling_mapping_file_path = scaling_dest_dir + "mapping" + self.path_seperator;
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
            path_src = filter_dest_dir + self.path_seperator + file;
            df = pd.DataFrame()
            if file.find("executed") is not -1:
                scaling_file_name += "-quote.csv"
                machine = ex.EX_Preprocess()
                df = machine.preprocess(path_src, self.int_sec)
                df = machine.scail(df)
                df.to_csv(scaling_dest_dir + scaling_file_name , index=False)
                file_count = file_count + 1
                print(str(file_count) + ' create... :' + self.scaling_dest_path + scaling_file_name)
            elif file.find("orderbook") is not -1:
                scaling_file_name += "-order.csv"
                machine = ob.OB_Preprocess()
                df = machine.preprocess(path_src, self.int_sec)
                df = machine.scail(df)
                df.to_csv(scaling_dest_dir + scaling_file_name, index=False)
                file_count = file_count + 1
                print(str(file_count) + ' create... :' + self.scaling_dest_path + scaling_file_name)

            mapping_df = mapping_df.append({'filter':file, 'scaling':scaling_file_name},ignore_index=True)
        mapping_df.to_csv(filter_scaling_mapping_file_path + "mapping.csv", index=False)
        return scaling_dest_dir, file_count


