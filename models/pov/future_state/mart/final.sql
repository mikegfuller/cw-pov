select * from {{ ref('stg_table_a') }}
union all
select * from {{ ref('stg_table_b') }}