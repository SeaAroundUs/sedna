import boto3
from sedna.common import read_sql_file, REGION_NAME, RESULT_CONFIGURATION


def run_query(sql):
    athena = boto3.client('athena', region_name=REGION_NAME)
    query = athena.start_query_execution(QueryString=sql, ResultConfiguration=RESULT_CONFIGURATION)
    return query['QueryExecutionId']


def get_query_results(qid):
    athena = boto3.client('athena', region_name=REGION_NAME)
    return athena.get_query_results(QueryExecutionId=qid)


def create_database():
    print('Creating database in Athena...')
    return run_query(read_sql_file('create_database.sql'))


# ddl for parquet tables: https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html
def create_core_tables():
    print('Creating core tables in Athena...')
    print('---')
    for schema in ['allocation', 'distribution', 'geo', 'master', 'recon', 'views']:
        print(f'-- {schema} --')
        queries = read_sql_file(f'tables/{schema}.sql').split(';')[:-1]
        for sql in queries:
            table_name = sql.strip().split('\n')[0].replace('-- ', '')
            print(f'Creating {table_name}...')
            run_query(sql)
    print('---')


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
def create_data_table():
    print('Creating data table in Athena...')
    sql = read_sql_file(f'create_dataraw_table.sql')
    run_query(sql)


def test_tables():
    pass
