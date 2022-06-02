{% set get_query_ddl %}
select select_statement from {{ ref('prep_view') }}
{% endset %}

{% set results = run_query(get_query_ddl) %}

{% if execute %}
{% set results_list = results.columns[0].values()|join(';') %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ results_list }}
