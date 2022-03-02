import boto3
import time
from sedna.common import read_sql_file, REGION_NAME, RESULT_CONFIGURATION


def run_query(sql):
    athena = boto3.client('athena', region_name=REGION_NAME)
    query = athena.start_query_execution(QueryString=sql, ResultConfiguration=RESULT_CONFIGURATION)
    return query['QueryExecutionId']


def get_query_results(qid):
    athena = boto3.client('athena', region_name=REGION_NAME)
    while True:
        query_exec = athena.get_query_execution(QueryExecutionId=qid)
        state = query_exec['QueryExecution']['Status']['State']
        if state not in ['QUEUED', 'RUNNING']:  # TODO handle error states
            break
        time.sleep(1)
    return athena.get_query_results(QueryExecutionId=qid)


def create_database():
    print('Creating database in Athena...')
    return run_query(read_sql_file('create_database.sql'))


# ddl for parquet tables: https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html
def create_core_tables():
    print('Creating core tables in Athena...\n---')
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
    sql = read_sql_file('create_dataraw_table.sql')
    run_query(sql)


def test_tables():
    print('Testing tables...\n---')
    sql = 'SHOW TABLES IN sedna;'
    qid = run_query(sql)
    result = get_query_results(qid)
    tables = [row['Data'][0]['VarCharValue'] for row in result['ResultSet']['Rows']]
    bad_tables = []
    for table in tables:
        print(f'Testing {table}...', end='')
        sql = f'SELECT * FROM sedna.{table} LIMIT 1;'
        qid = run_query(sql)
        try:
            result = get_query_results(qid)
            if len(result['ResultSet']['Rows']) == 2:  # column names count as a row
                print('OK!')
            else:
                print('ERROR: EMPTY TABLE!')
                bad_tables += [table]
        except Exception as err:  # TODO harden this, its not 100% reliable yet
            print(f'ERROR: QUERY FAILED! {err}')
            bad_tables += [table]
    if len(bad_tables) > 0:
        print('The following tables failed:', bad_tables)


def preprocess_raw_data():
    # substitute taxon key if there is no distribution
    # TODO
    pass
