import pandas as pd


def run():
    event_df = pd.read_csv('events.csv', parse_dates=['EventDatetime'])
    make_time_seres(event_df)


def make_time_seres(df):
    dict_event_type = {k: v for k, v in df.groupby('EventType')}
    df_sum = pd.DataFrame()
    for id_name, id_df in dict_event_type.items():
        id_df = id_df.dropna()
        id_df.set_index('EventDatetime', inplace=True)
        id_df = id_df.resample('1D').count()
        id_df = id_df.rename(columns={'EventType': id_name})
        df_sum = df_sum.append(id_df)
    df_sum = df_sum.sort_index()
    df_sum.reset_index(inplace=True)
    df_sum = dedup_with_rollup(df_sum, ['EventDatetime'])
    df_sum.fillna(0, inplace=True)
    df_sum = df_sum.set_index('EventDatetime')
    df_sum = df_sum.sort_index()

    df_sum.to_csv("id_time.csv")
    df_sum.cumsum().to_csv("id_time_cum.csv")


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
