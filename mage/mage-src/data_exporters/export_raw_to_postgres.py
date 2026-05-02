from psycopg2 import connect, sql

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from utils.connections import get_postgres_url


def _postgres_dsn_from_sqlalchemy_url(url: str) -> str:
    return url.replace('postgresql+psycopg2://', 'postgresql://', 1)


@data_exporter
def export_raw_to_postgres(manifest: dict, **kwargs):
    """
    Ingest raw KKBox CSV files into PostgreSQL using server-side COPY.
    """
    if not manifest or 'table_specs' not in manifest:
        raise ValueError('Manifest is empty or invalid. Expected key: table_specs')

    table_specs = manifest['table_specs']
    dsn = _postgres_dsn_from_sqlalchemy_url(get_postgres_url())
    load_summary = []

    with connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SET synchronous_commit TO OFF;')
            cursor.execute('TRUNCATE TABLE members, transactions, user_logs, train_label;')

            for spec in table_specs:
                table_name = spec['table_name']
                csv_path = spec['csv_path']
                columns = spec['columns']
                force_null_columns = spec.get('force_null', [])

                columns_sql = sql.SQL(', ').join([sql.Identifier(column) for column in columns])
                force_null_sql = sql.SQL(', ').join([sql.Identifier(column) for column in force_null_columns])
                copy_statement = sql.SQL(
                    """
                    COPY {table_name} ({columns})
                    FROM {csv_path}
                    WITH (
                        FORMAT csv,
                        HEADER true,
                        NULL '',
                        FORCE_NULL ({force_null_columns})
                    )
                    """
                ).format(
                    table_name=sql.Identifier(table_name),
                    columns=columns_sql,
                    csv_path=sql.Literal(csv_path),
                    force_null_columns=force_null_sql,
                )

                cursor.execute(copy_statement)
                status = cursor.statusmessage or 'COPY 0'
                copied_rows = int(status.split()[-1]) if status.startswith('COPY') else 0
                load_summary.append({'table_name': table_name, 'rows_copied': copied_rows})
                print(f"-- [ingest_raw_data] {table_name}: copied {copied_rows} rows")

    return {'load_summary': load_summary}
