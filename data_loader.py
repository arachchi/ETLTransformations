import json
import pandas as pd


def load_data_from_json_file(file_name):
    """
    Load json file to a panda dataframe
    :param file_name: json file name
    :return: panda dataframe
    """
    data_list = []
    with open(file_name, 'r') as f:
        line = f.readline()

        while line:
            data = json.loads(line)
            data_list.append(data)
            line = f.readline()

    df = pd.json_normalize(data_list)

    return df


def load_data_from_json_list(data_list):
    """
    Load json data list to a panda dataframe
    :param data_list: list of json
    :return: panda dataframe
    """
    df = pd.json_normalize(data_list)

    return df
