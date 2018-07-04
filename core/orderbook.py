import pandas as pd
import numpy as np

import datetime
import core.timefunc as tf

class OB_Preprocess():

    def __init__(self):
        self.col = self.makeCol()
        self.code = None
        self.date = None
        self.file_name = None

    def makeCol(self):
        col = []
        col.append('Code')
        col.append('Time(etrade)')
        col.append('Time(timestamp)')
        for num in range(10):
            col.append('SellHoga' + str(num+1))
        for num in range(10):
            col.append('SellOrder' + str(num+1))
        for num in range(10):
            col.append('BuyHoga' + str(num+1))
        for num in range(10):
            col.append('BuyOrder' + str(num+1))
        col.append('TotalBuy')
        col.append('TotalSell')
        col.append('Dongsi')
        col.append('Baebun')
        return col

    def preprocess(self,path_src,int_sec):
        self.file_name = path_src.split("\\")[-1]
        print(str(self.file_name) + " / Pre-processing......")

        self.code = self.file_name.split('_')[-2]
        self.date = self.file_name.split('_')[1]

        df_orderbook = pd.read_csv(path_src, encoding='cp949', names=self.col)
        df_orderbook.iloc[:, 2] = list(map(tf.timestamp2time, df_orderbook.iloc[:, 2]))
        df_orderbook = df_orderbook[df_orderbook.loc[:, "Dongsi"] == 1]

        total_data = {}

        check_time = df_orderbook.iloc[0, 2]
        check_time = datetime.time(check_time.hour, check_time.minute, check_time.second + 1)

        idx = 0

        while True:

            if idx == len(df_orderbook) - 1:
                break

            temp_time = df_orderbook.iloc[idx, 2]
            if tf.timeDiff(temp_time, check_time).days == 0:
                total_data[check_time] = df_orderbook.iloc[idx - 1].tolist()
                check_time = tf.addSecs(check_time, int_sec)
            else:
                idx = idx + 1

        df_result = pd.DataFrame(total_data).T
        df_result.columns = self.col

        return df_result

    def scail(self, df):
        print(str(self.file_name) + " / Scailing......")

        df_price = pd.read_csv('data/daily_stock_price_20180622_csv.csv', index_col=0, thousands=",")
        df_price.index = list(map(lambda x:x[1:], df_price.index))

        df_shareratio = pd.read_csv('data/stock_shareratio_20180627_csv.csv', index_col=0, thousands=",", encoding='cp949')
        df_shareratio.index = list(map(lambda x:x[1:], df_shareratio.index))

        mt_col = df_price.columns.get_loc(self.date)
        mt_idx = df_price.index.get_loc(self.code)
        yestday_price = df_price.iloc[mt_idx,mt_col - 1] #yesterday price
        market_shares = df_shareratio.loc[self.code][0] - df_shareratio.loc[self.code][2] #the shares in market

        df[df.columns[3:13]] = df[df.columns[3:13]] / yestday_price #SellHoga1~10
        df[df.columns[23:33]] = df[df.columns[23:33]] / yestday_price #BuyHoga 1~10

        df[df.columns[13:23]] = (df[df.columns[13:23]] / market_shares).applymap(np.log)
        df[df.columns[33:45]] = (df[df.columns[33:45]] / market_shares).applymap(np.log)

        return df











