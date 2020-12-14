import pandas as pd

from util import extract_url_levels, extract_datetime


def transform_to_url_summary(df):
    """
    Extract url from url column and transform to url_level_1, url_level_2, url_level_3
    :param df: dataframe with url column
    :return: filtered dataframe with  user_id, time_stamp, url_level1, url_level2, url_level3, activity
    """
    df.dropna(subset=['url', 'user.session_id'])
    url_extracted_df = df.apply(lambda row: extract_url_levels(row['url']), axis='columns', result_type='expand')
    url_extracted_df.rename(columns={0: 'url_level1', 1: 'url_level2', 2: 'url_level3'}, inplace=True)

    concatenated_df = pd.concat([df, url_extracted_df], axis='columns')
    concatenated_df.rename(columns={'user.session_id': 'user_id',
                                    'timestamp': 'time_stamp',
                                    'action': 'activity'}, inplace=True)

    columns = ['user_id', 'time_stamp', 'url_level1', 'url_level2', 'url_level3', 'activity']
    if not concatenated_df.empty:
        filtered_url_summary = concatenated_df.filter(columns, axis=1)
    else:
        filtered_url_summary = pd.DataFrame(columns=columns)

    return filtered_url_summary


def transform_to_action_summary(df):
    """
    Extract URLs and summarize activities per user
    :param df: dataframe
    :return: filtered dataframe with user_id, time_bucket, url_level1, activity, activity_count, user_count
    """
    df = df.dropna(subset=['url', 'timestamp', 'user.id'])
    url_extracted_df = df.apply(lambda row: extract_url_levels(row['url']), axis='columns', result_type='expand')
    url_extracted_df.rename(columns={0: 'url_level1', 1: 'url_level2', 2: 'url_level3'}, inplace=True)
    url_extracted_df = pd.concat([df, url_extracted_df], axis='columns')

    time_bucket_df = df.apply(lambda row: extract_datetime(row['timestamp']), axis='columns', result_type='expand')
    result_df = pd.concat([url_extracted_df, time_bucket_df], axis='columns')
    result_df.rename(columns={0: 'time_bucket', 'user.id': 'user_id', 'action': 'activity'}, inplace=True)

    if not result_df.empty:
        action_summary = result_df.groupby(['time_bucket', 'url_level1', 'url_level2', "activity"])["activity"].count()
        unique_users = result_df.groupby(['time_bucket', 'url_level1', 'url_level2', "activity"])["user_id"].nunique()

        result_df = pd.concat([action_summary, unique_users], axis='columns')
        result_df.rename(columns={'activity': 'activity_count', 'user_id': 'user_count'}, inplace=True)
        result_df.reset_index(inplace=True)
    else:
        columns = ['time_bucket', 'url_level1', 'url_level2', 'activity', 'activity_count', 'user_count']
        result_df = pd.DataFrame(columns=columns)
    return result_df
