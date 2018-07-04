import pandas as pd
import numpy as np
import datetime
import core.timefunc as tf


class EX_Preprocess():

    def __init__(self):
        self.col1 = self.makeCol1()
        self.col2 = self.makeCol2()

        self.file_name = None
        self.code = None
        self.date = None

    def makeCol1(self):
        col = []

        col.append('Code')
        col.append('Time(etrade)')
        col.append('Time(timestamp)')
        col.append('Price')
        col.append('Sign')
        col.append('Amounts')
        col.append('Open')
        col.append('Open Time')
        col.append('High')
        col.append('High Time')
        col.append('Low')
        col.append('Low Time')
        col.append('Cumulative Amounts')
        col.append('Cumulative Value')
        col.append('Limited')
        col.append('D-1 delta')
        col.append('D-1 delta(%)')
        col.append('Sell cumamts')
        col.append('Sell cumexecuted')
        col.append('Buy cumamts')
        col.append('Buy cumexecuted')
        col.append('Executed Strength')
        col.append('Weighted Price')
        col.append('Sell1')
        col.append('Buy1')
        col.append('Market Info')
        col.append('D-1 amounts')

        return col

    def makeCol2(self):
        col = []
        col.append('Price(last executed)')
        col.append('Buy executed')
        col.append('Buy Wt Price')
        col.append('Sell executed')
        col.append('Sell Wt Price')
        col.append('Total executed')
        col.append('Total Wt Price')
        col.append('Open')
        col.append('High')
        col.append('Low')
        return col

    def preprocess(self,path_src,int_sec):
        self.file_name = path_src.split("\\")[-1]
        self.code = self.file_name.split('_')[-2]
        self.date = self.file_name.split('_')[1]

        print(str(self.file_name) + " / Pre-processing......")

        df_executed = pd.read_csv(path_src, encoding='cp949', names=self.col1)
        df_executed.iloc[:, 2] = list(map(tf.timestamp2time, df_executed.iloc[:, 2]))

        total_data = {}

        check_time = df_executed.iloc[0, 2]
        check_time = datetime.time(check_time.hour, check_time.minute, check_time.second + 1)

        start_idx = 0
        end_idx = 0

        while True:

            temp_time = df_executed.iloc[end_idx, 2]

            if len(df_executed) - 1 == end_idx:
                break

            if tf.timeDiff(temp_time, check_time).days == 0:
                df_interval = df_executed.iloc[start_idx:end_idx]

                data_list = []

                try:
                    data_list.append(df_interval.iloc[-1, 3])  # last executed
                except:
                    data_list.append(np.nan)

                temp = df_interval[df_interval.loc[:, 'Sign'] == '+']
                if len(temp) == 0:
                    data_list.append(np.nan)
                    data_list.append(np.nan)
                else:
                    data_list.append(temp.loc[:, 'Amounts'].sum())  # Buy amts
                    data_list.append(
                        np.dot(np.array(temp.loc[:, 'Price']), np.array(temp.loc[:, 'Amounts'])) / temp.loc[:,
                                                                                                   'Amounts'].sum())  # Buy wt price

                temp = df_interval[df_interval.loc[:, 'Sign'] == '-']
                if len(temp) == 0:
                    data_list.append(np.nan)
                    data_list.append(np.nan)
                else:
                    data_list.append(temp.loc[:, 'Amounts'].sum())  # Sell amts
                    data_list.append(
                        np.dot(np.array(temp.loc[:, 'Price']), np.array(temp.loc[:, 'Amounts'])) / temp.loc[:,
                                                                                                   'Amounts'].sum())  # Sell wt price

                temp = df_interval[(df_interval.loc[:, 'Sign'] == '-') | (df_interval.loc[:, 'Sign'] == '+')]
                if len(temp) == 0:
                    data_list.append(np.nan)
                    data_list.append(np.nan)
                else:
                    data_list.append(temp.loc[:, 'Amounts'].sum())  # Total amts
                    data_list.append(
                        np.dot(np.array(temp.loc[:, 'Price']), np.array(temp.loc[:, 'Amounts'])) / temp.loc[:,
                                                                                                   'Amounts'].sum())  # Total wt price

                try:
                    data_list.append(df_interval.iloc[-1, 6])  # OPEN
                except:
                    data_list.append(np.nan)

                try:
                    data_list.append(df_interval.iloc[-1, 8])  # HIGH
                except:
                    data_list.append(np.nan)

                try:
                    data_list.append(df_interval.iloc[-1, 10])  # LOW
                except:
                    data_list.append(np.nan)

                total_data[check_time] = data_list

                check_time = tf.addSecs(check_time, int_sec)
                start_idx = end_idx

            else:
                end_idx = end_idx + 1

        df_result = pd.DataFrame(total_data).T
        df_result.columns = self.col2

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

        df[df.columns[0]] = df[df.columns[0]] / yestday_price
        df[df.columns[2]] = df[df.columns[2]] / yestday_price
        df[df.columns[4]] = df[df.columns[4]] / yestday_price
        df[df.columns[6:]] = df[df.columns[6:]] / yestday_price

        df[df.columns[1]] = (df[df.columns[1]] / market_shares).applymap(np.log)
        df[df.columns[3]] = (df[df.columns[3]] / market_shares).applymap(np.log)
        df[df.columns[5]] = (df[df.columns[5]] / market_shares).applymap(np.log)


        return df





