-- depends_on: {{ ref('attribute_tbl') }}
-- depends_on: {{ ref('customer_tbl') }}
-- depends_on: {{ ref('test_table_b') }}

{% call statement('result', fetch_result=True) %}

select source_column_name from {{ ref('attribute_tbl') }}
where table_name = 'test_table_b'
  
{% endcall %}

{%- set attributes = load_result('result').data -%}

{% for attribute in attributes %}

select cstmr_id as customer_id,
    (
        select attribute_id
        from {{ ref('attribute_tbl') }}
            inner join {{ ref('customer_tbl') }} on fk_table_id = table_id
        where table_name = 'test_table_b'
            and lower(SOURCE_COLUMN_NAME) = lower('{{attribute[0]}}')
    ) attribute_id,
    cast({{attribute[0]}} as varchar) attribute_value
from {{ ref('test_table_b') }}
where {{attribute[0]}} is not null

{%- if not loop.last %}
union all 
{% endif %}   
  
{% endfor %}

