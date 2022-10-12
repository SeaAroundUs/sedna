import boto3
import time
from collections import OrderedDict
from sedna.common import read_sql_file, REGION_NAME, RESULT_CONFIGURATION, ALLOCATION_RESULT_PREFIX, BUCKET_NAME
from sedna.s3 import folder_exists_and_not_empty

# CTA table : pre-req CTAS tables
CTAS = OrderedDict()
CTAS['dataraw'] = []
CTAS['allocation_simple_area'] = []
CTAS['simple_area_cell_assignment'] = ['allocation_simple_area']
CTAS['allocation_hybrid_area'] = ['dataraw']
CTAS['hybrid_to_simple_area_mapper'] = ['allocation_simple_area',
                                        'allocation_hybrid_area']
CTAS['predepth_data'] = ['dataraw',
                         'allocation_simple_area',
                         'allocation_hybrid_area']
CTAS['depth_adjustment_function_eligible_rows'] = ['predepth_data']
CTAS['depth_adjustment_function_area_possible_combos'] = ['simple_area_cell_assignment',
                                                          'allocation_simple_area']
CTAS['depth_adjustment_function_create_areas'] = ['depth_adjustment_function_area_possible_combos']
CTAS['depth_adjustment_function_area'] = ['predepth_data',
                                          'depth_adjustment_function_eligible_rows',
                                          'depth_adjustment_function_create_areas']
CTAS['data'] = ['predepth_data',
                'depth_adjustment_function_eligible_rows',
                'depth_adjustment_function_area']
CTAS['cells_for_area_type_3'] = ['simple_area_cell_assignment',
                                 'allocation_simple_area',
                                 'depth_adjustment_function_area']
CTAS['cells_for_generic_area'] = ['simple_area_cell_assignment',
                                  'hybrid_to_simple_area_mapper',
                                  'cells_for_area_type_3']
CTAS['allocation_unique_area'] = ['data']
CTAS['allocation_unique_area_cell'] = ['allocation_unique_area',
                                       'cells_for_generic_area']


def run_query(sql, output_location=None, *args):
    athena = boto3.client('athena', region_name=REGION_NAME)
    query_kwargs = {'QueryString': sql, 'ResultConfiguration': RESULT_CONFIGURATION}
    if output_location:
        query_kwargs['ResultConfiguration']['OutputLocation'] = output_location
    if len(args):
        query_kwargs['ExecutionParameters'] = args
    query = athena.start_query_execution(**query_kwargs)
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
        print('.', end='', flush=True)
        time.sleep(timeout)
        attempt += 1
    raise Exception(f'Ran out of tries waiting for {tables_display} ({tries} tries of {timeout}s);' +
                    'try increasing number of tries or timeout')


def create_database():
    print('Creating database in Athena...')
    sql = 'CREATE DATABASE IF NOT EXISTS sedna;'
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
        # TODO inject a comment at the top of the file with a filterable run value
        run_query(sql)


def get_fishing_entities():
    print('Grabbing fishing entities...', end='', flush=True)
    sql = '''
        -- listing fishing entities for allocations
        SELECT d.original_fishing_entity_id, fe.name, COUNT(1)
        FROM sedna.data d
        JOIN sedna.fishing_entity fe 
          ON (d.original_fishing_entity_id = fe.fishing_entity_id)
        GROUP BY 1, 2
        ORDER BY COUNT(1) ASC;
    '''
    qid = run_query(sql)
    result = get_query_results(qid)
    fishing_entities = list((row['Data'][0]['VarCharValue'], row['Data'][1]['VarCharValue'])
                            for row in result['ResultSet']['Rows'][1:])
    print(f'found {len(fishing_entities)} rows')
    return fishing_entities


def allocation_result(fishing_entity_id, name):
    export_folder = f'{ALLOCATION_RESULT_PREFIX}/fishing_entity_{fishing_entity_id:03}/'
    if folder_exists_and_not_empty(export_folder):
        print(f'Skipping {name} (ID {fishing_entity_id}); already exists')
        return
    print(f'Starting allocation for {name} (ID {fishing_entity_id})...')
    sql = read_sql_file('allocation.sql', True, fishing_entity_id=fishing_entity_id)
    start = time.perf_counter()
    qid = run_query(sql, f's3://{BUCKET_NAME}/{export_folder}', fishing_entity_id)
    get_query_results(qid)
    elapsed = time.perf_counter() - start
    print(f'{name} (ID {fishing_entity_id}) done! Execution took {elapsed:.2f}s')


def test_imported_tables():
    print('Testing imported tables...\n---')
    sql = 'SHOW TABLES IN sedna;'
    qid = run_query(sql)
    result = get_query_results(qid)
    tables = [row['Data'][0]['VarCharValue'] for row in result['ResultSet']['Rows']
              if row['Data'][0]['VarCharValue'] not in CTAS.keys()]
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


def drop_all_ctas_tables():
    for table in CTAS:
        sql = f'DROP TABLE sedna.{table};'
        run_query(sql)
