from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
    
@data_loader
def load_data_from_postgres(*args, **kwargs) -> DataFrame:
    # Query join thông tin user, giao dịch và nhãn churn
    query = """
        SELECT 
            t.msno, 
            t.is_churn, 
            m.city, 
            m.gender, 
            m.registered_via,
            tr.actual_amount_paid,
            tr.is_auto_renew
        FROM train_label t
        LEFT JOIN members m ON t.msno = m.msno
        LEFT JOIN transactions tr ON t.msno = tr.msno
        LIMIT 10000;
    """
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # print(type(loader.load(query)))
        return loader.load(query)


# from mage_ai.settings.repo import get_repo_path
# from mage_ai.io.config import ConfigFileLoader
# from mage_ai.io.postgres import Postgres
# from os import path

# if 'data_loader' not in globals():
#     from mage_ai.data_preparation.decorators import data_loader

# @data_loader
# def check_connection_and_list_tables(*args, **kwargs):
#     config_path = path.join(get_repo_path(), 'io_config.yaml')
#     config_profile = 'default'

#     # Query để lấy danh sách các bảng trong schema 'public'
#     query = """
#         SELECT COUNT(*) FROM user_logs;
#     """

#     try:
#         with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
#             df = loader.load(query)
            
#             # In ra danh sách các bảng để kiểm tra trong log
#             print("--- Kết nối thành công! ---")
#             print(df)
#             # print("Các bảng có trong database:")
#             # print(df['table_name'].tolist())
            
#             return df
#     except Exception as e:
#         print(f"--- Kết nối thất bại! ---")
#         print(f"Lỗi: {e}")
#         return None