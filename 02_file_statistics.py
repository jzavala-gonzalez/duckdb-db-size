import os
import duckdb

from constants import data_dir


# Walk through the uncompressed data directory
filepaths = []
for dirpath, dirnames, filenames in os.walk(data_dir):
    for file in filenames:
        fpath = os.path.join(dirpath, file)
        filepaths.append(fpath)
filepaths.sort()

num_files = len(filepaths)
print(f'{num_files:,} files in dataset')
print()

print('Example filepath:')
print(filepaths[0])
print()

filesize_data = []
for filepath in filepaths:
    filesize = os.path.getsize(filepath) # size in bytes
    filesize_data.append((filepath, filesize))

# Save filesize data to a DuckDB database
os.makedirs('databases', exist_ok=True)
con = duckdb.connect('databases/files.db')
con.execute('CREATE TABLE file_sizes (filepath VARCHAR, filesize INT)')
con.executemany('INSERT INTO file_sizes VALUES (?, ?)', filesize_data)

print('file_sizes:')
print(con.sql('SELECT * FROM file_sizes LIMIT 5'))

print('Summary:')
print(con.sql(
'''
select
    count(*) as "Total files",
    (mean(filesize)/1e3).round(3) as "Mean size (kB)",
    (stddev_pop(filesize)/1e3).round(4) as "Size Std dev (kB)",
    (sum(filesize)/1e6).round(1) as "Total size (MB)",
from file_sizes
'''
))
con.close()