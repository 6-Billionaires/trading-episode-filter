import os
import pandas as pd
import core.orderbook as ob
import core.executed as ex
import config

path_seperator = "/"
int_sec = 1
src_path = config.PATH_CONFIG['src_path']
filter_dest_path = config.PATH_CONFIG['filter_dest_path']
scaling_dest_path = config.PATH_CONFIG['scaling_dest_path']

def create_scaling_episode_file(filter_dest_dir = filter_dest_path+"01", episode_type = "01"):
    file_list = os.listdir(filter_dest_dir)

    scaling_dest_dir = scaling_dest_path + episode_type + path_seperator
    filter_scaling_mapping_file_path = scaling_dest_dir + "mapping" + path_seperator;
    if os.path.exists(filter_scaling_mapping_file_path) is False:
        os.makedirs(filter_scaling_mapping_file_path)

    file_count = 0
    mapping_df = pd.DataFrame(columns=['filter', 'scaling'])
    for file in file_list:
        file_split = file.split("_")
        type_str = file_split[0]
        date_str = file_split[1].replace("-", "")
        code_str = file_split[3]
        scaling_file_name = type_str + "-" + code_str + "-" + date_str
        path_src = filter_dest_dir + path_seperator + file;
        df = pd.DataFrame()
        if file.find("executed") is not -1:
            scaling_file_name += "-quote.csv"
            machine = ex.EX_Preprocess()
            df = machine.preprocess(path_src, int_sec)
            try:
                df = machine.scail(df)
            except KeyError as e:
                print(e)
            df.to_csv(scaling_dest_dir + scaling_file_name, index=False)
            file_count = file_count + 1
            print(str(file_count) + ' create... :' + scaling_dest_path + scaling_file_name)
        elif file.find("orderbook") is not -1:
            scaling_file_name += "-order.csv"
            machine = ob.OB_Preprocess()
            df = machine.preprocess(path_src, int_sec)
            try:
                df = machine.scail(df)
            except KeyError as e:
                print(e)
            df.to_csv(scaling_dest_dir + scaling_file_name, index=False)
            file_count = file_count + 1
            print(str(file_count) + ' create... :' + scaling_dest_path + scaling_file_name)

        mapping_df = mapping_df.append({'filter': file, 'scaling': scaling_file_name}, ignore_index=True)
    mapping_df.to_csv(filter_scaling_mapping_file_path + "mapping.csv", index=False)
    return scaling_dest_dir, file_count

scaling_dest_path, file_count = create_scaling_episode_file()
print("Episode[" + scaling_dest_path + "], Files["+str(file_count) + "] created.")