import os
import duckdb
import polars as pl
import shutil

from constants import data_dir

num_starting_files_options = [1000, 3000, 5000, 7000, 9000,
                              ] # how many files ingested at creation time
                                   # must be at least 1
batch_size = 2000 # how many files to ingest at a time, usually 1 or 2

# Get file info
con_files = duckdb.connect('databases/files.db')

# Retrieve filepaths
filepaths = con_files.sql('SELECT filepath FROM file_sizes').fetchall()
filepaths = [row[0] for row in filepaths]
num_files = len(filepaths)
print(f'{num_files:,} files in dataset')
print()


con_files.close()

# Iterate?
initial_data_dir = f'initial_{data_dir}'
incremental_data_dir = f'incremental_{data_dir}'
tmp_db_path = 'databases/tmp.db'
sizes_db_path = 'databases/sizes.db'
all_db_sizes_df = None
for (idx_starting_files, num_starting_files) in enumerate(num_starting_files_options, start=0):

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
    print(f'Total rows: {con.sql("select count(*) from regions_without_service_staging").fetchone()[0]:,}')
    pragma_db_size = con.sql('call pragma_database_size();')
    print(pragma_db_size)
    pragma_db_size_df = pragma_db_size.pl()
    pragma_db_size_df = (
        pragma_db_size_df
        .select([
            pl.lit(num_starting_files).alias('num_starting_files'),
            pl.lit(0).alias('num_batches_added'),
            pl.lit(batch_size).alias('batch_size'),
            pl.lit(con.sql("select count(*) from regions_without_service_staging").fetchone()[0]).alias('num_files'),
            *pragma_db_size_df.columns,
        ])
    )
    print(pragma_db_size_df)
    if all_db_sizes_df is None:
        all_db_sizes_df = pragma_db_size_df
    else:
        all_db_sizes_df = all_db_sizes_df.vstack(pragma_db_size_df)
    con.close()

    ### Batch incremental datasets
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

    for batch_num, (start, end) in enumerate(batch_indexes, start=1):
        print(f'Batch {batch_num}/{num_batches}')
        print(f'Files {start:,}-{end:,}')

        if os.path.exists(incremental_data_dir):
            shutil.rmtree(incremental_data_dir)
        filepaths_incremental = incremental_files[start:end]
        for fpath in filepaths_incremental:
            new_filepath = 'incremental_' + fpath
            os.makedirs(os.path.dirname(new_filepath), exist_ok=True)
            shutil.copy(fpath, new_filepath)

        # Ingest batch
        con = duckdb.connect(tmp_db_path)
        con.execute(
        f'''
        create or replace TEMP table local_raw_regions_without_service as (
            select *
            from read_json('{incremental_data_dir}/*/*.json', filename=true, auto_detect=true, format='auto') 
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
        con.execute('''
        create or replace table regions_without_service_staging as (
        with initial_union as (
            select *
            from regions_without_service_staging
            union all
            select *
            from local_regions_without_service_staging
        ),

        regions_with_dupe_counts as (
            select
                *,
                row_number() over (partition by object_key) as object_key_copy_number
            from initial_union
        )

        select * exclude (object_key_copy_number)
        from regions_with_dupe_counts
        where object_key_copy_number = 1
        order by object_key
        );
        ''')
        con.close()

        # Measure new database size
        con = duckdb.connect(tmp_db_path)
        print(f'Total rows: {con.sql("select count(*) from regions_without_service_staging").fetchone()[0]:,}')
        pragma_db_size = con.sql('call pragma_database_size();')
        print(pragma_db_size)
        pragma_db_size_df = pragma_db_size.pl()
        pragma_db_size_df = (
            pragma_db_size_df
            .select([
                pl.lit(num_starting_files).alias('num_starting_files'),
                pl.lit(batch_num).alias('num_batches_added'),
                pl.lit(batch_size).alias('batch_size'),
                pl.lit(con.sql("select count(*) from regions_without_service_staging").fetchone()[0]).alias('num_files'),
                *pragma_db_size_df.columns,
            ])
        )
        all_db_sizes_df = all_db_sizes_df.vstack(pragma_db_size_df)
        con.close()

        print()
    
    # con.close()

# print(all_db_sizes_df)

# Save database size data to a DuckDB database
con_sizes = duckdb.connect(sizes_db_path)
con_sizes.execute(
'''
create or replace table db_sizes as (
    select *
    from all_db_sizes_df
)
'''
)
print(con_sizes.sql('select * from db_sizes'))
con_sizes.close()

if os.path.exists(incremental_data_dir):
    shutil.rmtree(incremental_data_dir)
if os.path.exists(initial_data_dir):
    shutil.rmtree(initial_data_dir)