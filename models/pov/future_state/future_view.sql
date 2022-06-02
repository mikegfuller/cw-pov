select cstmr_id as customer_id,
    (
        select attribute_id
        from {{ ref('attribute_tbl') }}
            inner join {{ ref('customer_tbl') }} on fk_table_id = table_id
        where table_name = 'test_table_a'
            and lower(SOURCE_COLUMN_NAME) = lower('test_column_a')
    ) attribute_id,
    cast(test_column_a as varchar) attribute_value
from {{ ref('test_table_a') }}
where test_column_a is not null
union all
select cstmr_id as customer_id,
    (
        select attribute_id
        from {{ ref('attribute_tbl') }}
            inner join {{ ref('customer_tbl') }} on fk_table_id = table_id
        where table_name = 'test_table_b'
            and lower(SOURCE_COLUMN_NAME) = lower('test_column_b')
    ) attribute_id,
    cast(test_column_b as varchar) attribute_value
from {{ ref('test_table_b') }}
where test_column_b is not null