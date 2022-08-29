import boto3
import time
from collections import OrderedDict
from sedna.common import read_sql_file, REGION_NAME, RESULT_CONFIGURATION

# CTA table : pre-req CTAS tables
CTAS = OrderedDict()
CTAS['dataraw'] = []
CTAS['allocation_simple_area'] = []
CTAS['simple_area_cell_assignment'] = ['allocation_simple_area']
CTAS['allocation_hybrid_area'] = ['dataraw']
CTAS['hybrid_to_simple_area_mapper'] = ['allocation_simple_area', 'allocation_hybrid_area']
CTAS['predepth_data'] = ['dataraw', 'allocation_simple_area', 'allocation_hybrid_area']
CTAS['depth_adjustment_function_eligible_rows'] = ['predepth_data']
CTAS['depth_adjustment_function_create_areas'] = ['allocation_simple_area', 'simple_area_cell_assignment']
CTAS['depth_adjustment_function_area'] = ['predepth_data', 'depth_adjustment_function_eligible_rows',
                                          'depth_adjustment_function_create_areas']
# TODO CTAS['data'] = ['predepth_data', 'depth_adjustment_function_eligible_rows', 'depth_adjustment_function_area']

# TODO THEN (this updates the autogen tables), is this one needed? is it already done?
# TODO EXEC Allocation_Populate_SpatialAutoGenTables
# TODO https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L63

# CTAS['allocation_unique_area'] = ['data']
# TODO CTAS['cells_for_area_type_3'] = []
# TODO CTAS['allocation_unique_area_cell'] = ['']


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


def wait_for_tables(tables, tries=60, timeout=30):
    if len(tables) == 0:
        return  # early return if nothing to wait on
    tables_display = ', '.join(tables)
    tables_regex = '|'.join(tables)
    print(f'Waiting for creation of {tables_display} table(s) to finish...', end='', flush=True)
    sql = f"SHOW TABLES IN sedna '{tables_regex}';"
    attempt = 0
    while attempt < tries:
        qid = run_query(sql)
        result = get_query_results(qid)
        if len(result['ResultSet']['Rows']) == len(tables):
            print('done!')
            return
        print('.', end='')
        time.sleep(timeout)
        attempt += 1
    raise Exception(f'Ran out of tries waiting for {tables_display} ({tries} tries of {timeout}s);' +
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


# ctas reference: https://docs.aws.amazon.com/athena/latest/ug/ctas.html
# !!! NOTE !!! if this table needs to be recreated for a run then underlying
#              ctas.<table> folder must be deleted in S3 as well
def create_all_ctas_tables():
    for table in CTAS:
        reqs = CTAS[table]
        wait_for_tables(reqs)
        print(f'Creating {table} from query...')
        sql = read_sql_file(f'ctas/{table}.sql')
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
