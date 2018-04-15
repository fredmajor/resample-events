import pandas as pd


def run():
    df = pd.read_csv('events.csv', parse_dates=['EventDatetime'])
    df_time = make_time_seres(df)
    df_time.to_csv("events_time.csv")
    df_time.cumsum().to_csv("events_time_cumulative.csv")


def make_time_seres(df):
    dict_event_type = {k: v for k, v in df.groupby('EventType')}
    df_sum = pd.DataFrame()
    for type_name, type_df in dict_event_type.items():
        type_df.dropna(inplace=True)
        type_df.set_index('EventDatetime', inplace=True)
        type_df = type_df.resample('1D').count()
        type_df = type_df.rename(columns={'EventType': type_name})
        df_sum = df_sum.append(type_df)
    df_sum = df_sum.sort_index()
    df_sum.reset_index(inplace=True)
    df_sum = dedup_with_rollup(df_sum, ['EventDatetime'])
    df_sum.fillna(0, inplace=True)
    return df_sum.set_index('EventDatetime')


def dedup_with_rollup(df, distinct_cols):
    count_before = df.shape[0]
    print("Will perform deduplication with data rollup now. "
          "Record count before={}, distinct key={}".format(count_before, distinct_cols))
    distinct_cols = list(distinct_cols)
    # #######################################
    # dirty fix for one-element distinct_key
    new_col = ""
    if len(distinct_cols) == 1:
        new_col = distinct_cols[0]
        new_col += "_copy_hack"
        df[new_col] = df[distinct_cols[0]]
        distinct_cols.append(new_col)
    #########################################
    all_cols = list(df)
    df = df.groupby(distinct_cols, as_index=False)[all_cols].last()
    if new_col:
        del df[new_col]
    count_after = df.shape[0]
    print("Did perform deduplication. Record count after={}. Dropped rows={}"
          .format(count_after, count_before - count_after))
    return df


if __name__ == "__main__":
    run()
