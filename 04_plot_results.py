import duckdb
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

con = duckdb.connect('databases/sizes.db')

(con.execute(
'''
create or replace view db_sizes_detailed as (
select *,
    regexp_extract(database_size, '^([0-9\.]+)[A-Z]+$', 1) as database_size_number,
    regexp_extract(database_size, '^[0-9\.]+([A-Z]+)$', 1) as database_size_unit,
    (case when database_size_unit = 'KB' then 1e3
         when database_size_unit = 'MB' then 1e6
         when database_size_unit = 'GB' then 1e9
         else NULL end)::integer
         as database_size_multiplier, -- sets to bytes
    (database_size_number::float * database_size_multiplier).round(0) as database_size_bytes,
    (database_size_bytes / 1e6).round(3) as database_size_mb,

    num_batches_added = 0 as is_initial_database,
from db_sizes
)
'''
))
print(con.sql('select * from db_sizes_detailed'))
sizes_df = con.sql('select * from db_sizes_detailed').pl()
con.close()

sizes_df_initial = sizes_df.filter(pl.col('is_initial_database'))
sizes_df_initial_num_files = sizes_df_initial.select('num_files').to_series().to_list()
sizes_df_initial_mb = sizes_df_initial.select('database_size_mb').to_series().to_list()

distinct_starting_files = sizes_df_initial_num_files # same thing

# fig = px.line(sizes_df,
#                  y='database_size_mb',
#                  x='num_files',
#                  symbol='is_initial_database',
#                  color='is_initial_database',
#                  hover_name='database_name',
#                  markers=True,
#         )
# fig.show()

fig = go.Figure()

# # Add traces
# fig.add_trace(go.Scatter(x=random_x, y=random_y0,
#                     mode='markers',
#                     name='markers'))



for num_starting_files in distinct_starting_files:
    sizes_df_subset = sizes_df.filter(pl.col('num_starting_files') == num_starting_files)
    sizes_df_subset_num_files = sizes_df_subset.select('num_files').to_series().to_list()
    sizes_df_subset_mb = sizes_df_subset.select('database_size_mb').to_series().to_list()

    fig.add_trace(go.Scatter(x=sizes_df_subset_num_files, y=sizes_df_subset_mb,
                    mode='lines+markers',
                    name='lines+markers',
                    line=dict(color='grey', width=1),
                ))
    pass

fig.add_trace(go.Scatter(x=sizes_df_initial_num_files, y=sizes_df_initial_mb,
                    mode='lines+markers',
                    name='lines+markers'))
# fig.add_trace(go.Scatter(x=random_x, y=random_y2,
#                     mode='lines',
#                     name='lines'))

fig.show()