import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


RAW_TABLE_SPECS = [
    {
        'table_name': 'members',
        'csv_file': 'members_v3.csv',
        'columns': [
            'msno',
            'city',
            'bd',
            'gender',
            'registered_via',
            'registration_init_time',
        ],
        'force_null': ['city', 'bd', 'gender', 'registered_via', 'registration_init_time'],
    },
    {
        'table_name': 'train_label',
        'csv_file': 'train_v2.csv',
        'columns': ['msno', 'is_churn'],
        'force_null': ['is_churn'],
    },
    {
        'table_name': 'transactions',
        'csv_file': 'transactions_v2.csv',
        'columns': [
            'msno',
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',
            'is_auto_renew',
            'transaction_date',
            'membership_expire_date',
            'is_cancel',
        ],
        'force_null': [
            'payment_method_id',
            'payment_plan_days',
            'plan_list_price',
            'actual_amount_paid',
            'is_auto_renew',
            'transaction_date',
            'membership_expire_date',
            'is_cancel',
        ],
    },
    {
        'table_name': 'user_logs',
        'csv_file': 'user_logs_v2.csv',
        'columns': [
            'msno',
            'date',
            'num_25',
            'num_50',
            'num_75',
            'num_985',
            'num_100',
            'num_unq',
            'total_secs',
        ],
        'force_null': ['date', 'num_25', 'num_50', 'num_75', 'num_985', 'num_100', 'num_unq', 'total_secs'],
    },
]


@data_loader
def load_kkbox_csv(*args, **kwargs):
    """
    Build ingest manifest for KKBox raw CSV files.
    CSV files are read by PostgreSQL server-side COPY from /var/lib/postgresql/raw_data.
    """
    raw_data_dir = kwargs.get('raw_data_dir', '/var/lib/postgresql/raw_data')

    table_specs = []
    for item in RAW_TABLE_SPECS:
        table_specs.append(
            {
                'table_name': item['table_name'],
                'csv_path': f"{raw_data_dir}/{item['csv_file']}",
                'columns': item['columns'],
                'force_null': item['force_null'],
            }
        )

    manifest_df = pl.DataFrame(
        {
            'table_name': [item['table_name'] for item in table_specs],
            'csv_path': [item['csv_path'] for item in table_specs],
            'column_count': [len(item['columns']) for item in table_specs],
        }
    )
    print('-- [ingest_raw_data] ingest manifest')
    print(manifest_df)

    return {'table_specs': table_specs}
