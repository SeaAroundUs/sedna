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
    return run_query(read_sql_file('create_database.sql'))


# ddl for parquet tables: https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html
def create_tables():
    print('Creating tables in Athena...')
    for schema in ['allocation', 'distribution', 'geo', 'master', 'recon']:
        print(f'-- {schema} --')
        queries = read_sql_file(f'tables/{schema}.sql').split(';')[:-1]
        for sql in queries:
            table_name = sql.split('\n')[0].replace('-- ', '')
            print(f'Creating {table_name}...')
            # run_query(sql)


# TODO need to create a data catalog?
