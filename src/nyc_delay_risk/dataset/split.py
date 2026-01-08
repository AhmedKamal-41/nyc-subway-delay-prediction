def time_split(df):
    """Split dataframe by time order: train 70%, val 15%, test 15%.
    
    Assumes dataframe is already sorted by bucket_start.
    """
    df = df.sort_values('bucket_start').reset_index(drop=True)
    
    total_rows = len(df)
    train_end = int(total_rows * 0.70)
    val_end = int(total_rows * 0.85)
    
    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()
    
    return train_df, val_df, test_df

