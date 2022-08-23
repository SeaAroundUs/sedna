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


def wait_for_table(table, tries=24, timeout=5):
    print(f'Waiting for creation of {table} table to finish...', end='', flush=True)
    sql = f"SHOW TABLES IN sedna '{table}';"
    attempt = 0
    while attempt < tries:
        qid = run_query(sql)
        result = get_query_results(qid)
        if len(result['ResultSet']['Rows']) > 0:
            print('done!')
            return
        print('.', end='')
        time.sleep(timeout)
        attempt += 1
    raise Exception(f'Ran out of tries waiting for {table} ({tries} tries of {timeout}s);' +
                    'try increasing number of tries or timeout')


def create_database():
    print('Creating database in Athena...')
    sql = read_sql_file('create_database.sql')
    run_query(sql)


# ddl for parquet tables: https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html
def create_core_tables():
    print('Creating core tables in Athena...\n---')
    for schema in ['allocation', 'distribution', 'geo', 'master', 'recon', 'views']:
        print(f'-- {schema} --')
        queries = read_sql_file(f'tables/{schema}.sql').split(';')[:-1]
        for sql in queries:
            table_name = sql.strip().split('\n')[0].replace('-- ', '')
            print(f'Creating {table_name} from snapshot...')
            run_query(sql)
    print('---')

# TODO DRY UP THESE CTAS!

# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.dataraw folder must be deleted in S3 as well
def create_dataraw_table():
    print('Creating dataraw table from query...')
    sql = read_sql_file('create_dataraw_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.allocation_simple_area folder must be deleted in S3 as well
def create_allocation_simple_area_table():
    print('Creating allocation simple area table from query...')
    sql = read_sql_file('create_allocation_simple_area_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.allocation_hybrid_area folder must be deleted in S3 as well
def create_allocation_hybrid_area_table():
    wait_for_table('dataraw')
    print('Creating allocation hybrid area table from query...')
    sql = read_sql_file('create_allocation_hybrid_area_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.data folder must be deleted in S3 as well
def create_data_table():
    wait_for_table('allocation_hybrid_area')
    wait_for_table('allocation_simple_area')
    wait_for_table('dataraw')
    print('Creating data table from query (this one takes a while)...')
    sql = read_sql_file('create_data_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.allocation_unique_area folder must be deleted in S3 as well
def create_allocation_unique_area_table():
    wait_for_table('data', tries=20, timeout=30)
    print('Creating allocation unique area table from query...')
    sql = read_sql_file('create_allocation_unique_area_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.allocation_unique_area folder must be deleted in S3 as well
def create_allocation_unique_area_cell_table():
    wait_for_table('allocation_unique_area')
    wait_for_table('simple_area_cell_assignment')
    wait_for_table('hybrid_to_simple_area_mapper')
    wait_for_table('cells_for_area_type_3')
    print('Creating allocation unique area cell table from query...')
    sql = read_sql_file('create_allocation_unique_area_cell_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.simple_area_cell_assignment folder must be deleted in S3 as well
def create_simple_area_cell_assignment_table():
    wait_for_table('allocation_simple_area')
    print('Creating simple_area_cell_assignment table from query...')
    sql = read_sql_file('create_simple_area_cell_assignment_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.hybrid_to_simple_area_mapper folder must be deleted in S3 as well
def create_hybrid_to_simple_area_mapper_table():
    wait_for_table('allocation_simple_area')
    wait_for_table('allocation_hybrid_area')
    print('Creating hybrid_to_simple_area_mapper table from query...')
    sql = read_sql_file('create_hybrid_to_simple_area_mapper_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.depth_adjustment_function_area folder must be deleted in S3 as well
def create_depth_adjustment_function_area_table():
    # TODO wait for tables
    print('Creating depth_adjustment_function_area table from query...')
    sql = read_sql_file('create_depth_adjustment_function_area_table.sql')
    run_query(sql)


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.cells_for_area_type_3 folder must be deleted in S3 as well
def create_cells_for_area_type_3_table():
    wait_for_table('simple_area_cell_assignment')
    wait_for_table('depth_adjustment_function_area')
    print('Creating cells_for_area_type_3 table from query...')
    sql = read_sql_file('create_cells_for_area_type_3_table.sql')
    run_query(sql)

def test_tables():
    print('Testing tables...\n---')
    sql = 'SHOW TABLES IN sedna;'
    qid = run_query(sql)
    result = get_query_results(qid)
    tables = [row['Data'][0]['VarCharValue'] for row in result['ResultSet']['Rows']]
    bad_tables = []
    for table in tables:
        print(f'Testing {table}...', end='', flush=True)
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
