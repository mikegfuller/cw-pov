{% set sql_query %}
        select
            'select cstmr_id as customer_id,
                (
                    select attribute_id
                    from {{ ref('attribute_tbl') }}
                        inner join {{ ref('customer_tbl') }} on fk_table_id = table_id
                    where table_name = '''|| table_name ||''' 
                        and lower(SOURCE_COLUMN_NAME) = lower(''' || source_column_name ||''')
                ) attribute_id,
                cast('''|| source_column_name || ''' as varchar) attribute_value 
            from  '|| table_name || '
            where ' || source_column_name || ' is not null'
        from {{ ref('attribute_tbl') }}
    {% endset %}

    {% if execute %}

        {% set query_list = run_query(sql_query).columns[0].values() %}

        {{ log(query_list, true) }}

        {% set final_stmt = query_list | join(' union all ') %}
        
    {% endif %}

{{final_stmt}}