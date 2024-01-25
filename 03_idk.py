import os
import duckdb
import shutil

from constants import data_dir

num_starting_files_options = [1, ] # how many files ingested at creation time
                                   # must be at least 1
batch_size = 3000 # how many files to ingest at a time, usually 1 or 2

# Get file info
con_files = duckdb.connect('databases/files.db')

# Retrieve filepaths
filepaths = con_files.sql('SELECT filepath FROM file_sizes').fetchall()
filepaths = [row[0] for row in filepaths]
num_files = len(filepaths)
print(f'{num_files:,} files in dataset')


con_files.close()

# Iterate?
initial_data_dir = f'initial_{data_dir}'
incremental_data_dir = f'incremental_{data_dir}'
tmp_db_path = 'databases/tmp.db'
for num_starting_files in num_starting_files_options:

    # Create initial dataset
    if os.path.exists(initial_data_dir):
        shutil.rmtree(initial_data_dir)
    filepaths_initial = filepaths[:num_starting_files]
    for fpath in filepaths_initial:
        new_filepath = 'initial_' + fpath
        os.makedirs(os.path.dirname(new_filepath), exist_ok=True)
        shutil.copy(fpath, new_filepath)

    # Create database
    if os.path.exists(tmp_db_path):
        os.remove(tmp_db_path)
    con = duckdb.connect(tmp_db_path)

    ### Create initial table
    con.execute(
    f'''
    create or replace TEMP table local_raw_regions_without_service as (
        select *
        from read_json('{initial_data_dir}/*/*.json', filename=true, auto_detect=true, format='auto') 
    )
    ''')
    con.execute(
    '''
    create or replace TEMP table local_regions_without_service_staging as (
        select
            strptime("timestamp", '%m/%d/%Y %I:%M %p') as "marca_hora_presentada",
            filename
                .string_split('__')[2]
                .regexp_extract('(.*).json', 1)
                .strptime('%Y-%m-%dT%H-%M-%S%z')
                ::TIMESTAMP -- drop timezone
                as "marca_hora_accedida",
            regions,
            totals,
            filename as object_key,
        from local_raw_regions_without_service
    );
    ''')
    # print(con.sql('from local_regions_without_service_staging'))
    con.execute('''
    create table regions_without_service_staging as (
        select *
        from local_regions_without_service_staging
        order by object_key
    )
    ''')
    ###
    con.close()

    con = duckdb.connect(tmp_db_path)
    print('Initial database size:')
    print(con.sql('call pragma_database_size();'))
    con.close()

    ### Create incremental datasets
    incremental_files = filepaths[num_starting_files:]
    num_incremental_files = len(incremental_files)
    batch_indexes = [
        (i, i+batch_size)
        for i in range(0, num_incremental_files, batch_size)
        if i+batch_size <= num_incremental_files
    ]
    num_batches = len(batch_indexes)
    # print(batch_indexes)
    print(f'{num_batches:,} batches of {batch_size:,} files each')
    
    # con.close()

shutil.rmtree(initial_data_dir)