from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_df(df: DataFrame, *args, **kwargs) -> DataFrame:
    # print(df)
    
    # 1. Xử lý missing values
    df['gender'] = df['gender'].fillna('unknown')
    df['city'] = df['city'].fillna(0).astype(int)
    df['actual_amount_paid'] = df['actual_amount_paid'].fillna(0.0)
    
    # 2. Xóa các duplicate phát sinh do join 
    # (Một user có thể có nhiều transaction, tạm thời lấy bản ghi đầu tiên)
    df = df.drop_duplicates(subset=['msno'], keep='last')
    
    # Ở bước thực tế, bạn sẽ cần code aggregate (groupby) cho transactions và user_logs phức tạp hơn.
    return df
