-- depends_on: {{ ref('attribute_tbl') }}

{% call statement('result', fetch_result=True) %}

select attribute_id, table_name, source_column_name from {{ ref('attribute_tbl') }}
  
{% endcall %}

{%- set attributes = load_result('result').data -%}

{{ log(attributes, info=True) }}

{% for attribute in attributes %}

select {{attribute[0]}} as atr_id, '{{attribute[1]}}' as atr_table_name

{%- if not loop.last %}
union all 
{% endif %}         
  
{% endfor %}