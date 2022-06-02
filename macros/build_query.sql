{% macro build_query() %}

    {% set sql_query %}
        select 
            'select ' || attribute_id || ' as atr_id, '''|| table_name ||''' as atr_table_name' as query_col
        from {{ ref('attribute_tbl') }}
    {% endset %}

    {% if execute %}

        {% set query_list = run_query(sql_query).columns[0].values() %}

        {{ log(query_list, true) }}

        {% set final_stmt = query_list | join(' union all ') %}

        {{ return(final_stmt) }}
        
    {% endif %}

{% endmacro %}