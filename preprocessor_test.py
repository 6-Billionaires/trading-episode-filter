from core import episode_filter as ef
import pandas as pd

def main(start_date, end_date, min_rate, max_rate, episode_type):


    pp = ef.Episod_filter()

    path_seperator = "/"
    int_sec = 1
    src_path = './output/source/'
    filter_dest_path = './output/target/filter_output/'
    scaling_dest_path = './output/target/scaling_output/'

    pp.setup(path_seperator, src_path, filter_dest_path, scaling_dest_path)

    # df = pp.get_df_toDb(start_date, end_date, min_rate, max_rate)
    df = pd.read_csv("./output/test/selectMaxTicker3.csv", dtype={'code': str, 'date': 'str', "type": str})
    df = df.fillna("")
    print('sql result : ' + str(len(df)))

    file_count, filter_dest_dir = pp.create_episode_file(df, episode_type)

    print("Episode[" + filter_dest_dir + "], Files["+str(file_count) + "] created.")

    scaling_dest_path, file_count = pp.create_scaling_episode_file(filter_dest_dir, episode_type)
    print("Episode[" + scaling_dest_path + "], Files["+str(file_count) + "] created.")

if __name__ == "__main__":
    main('20180510','20180510','25','30', '01')
