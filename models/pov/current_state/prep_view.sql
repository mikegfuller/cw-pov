select engine_name,  listagg(col1,'\n union all ')
within group (order by col1 asc) as select_statement
from (
 select 'select cstmr_id as customer_id, (select attribute_id from {{ref('attribute_tbl')}} \n inner join {{ref('customer_tbl')}}
on fk_table_id=table_id where table_name=\'' || table_name || '\' and lower(SOURCE_COLUMN_NAME) = lower(\''
      || SOURCE_COLUMN_NAME || '\')) attribute_id , cast(' || SOURCE_COLUMN_NAME || ' as varchar) attribute_value  from ' ||database_name||'.'||schema_name||'.'|| table_name ||' where '
	  || SOURCE_COLUMN_NAME || ' is not null'  as col1, m_tbl.engine_name
  from {{ref('attribute_tbl')}} m_attr
  inner join {{ref('customer_tbl')}} m_tbl on m_attr.fk_table_id=m_tbl.table_id
  where  lower(is_transform_attribute)= lower('n'))
group by engine_name
UNION ALL
-- Transform select MDM and SEGMENT
select engine_name,  listagg(col1,'\n union all ') within group (order by col1 asc) as create_statement from (  select 'select cstmr_id as customer_id, (select attribute_id from {{ref('attribute_tbl')}} \n inner join {{ref('customer_tbl')}}
on fk_table_id=table_id where table_name=\'' || table_name || '\' and lower(source_column_name) = lower(\''
      || source_column_name || '\')) attribute_id , cast(' || nvl(transformation_rule,source_column_name) || ' as varchar) attribute_value  from ' ||database_name||'.'||schema_name||'.'|| table_name
||' where attribute_value is not null'  as col1, m_tbl.engine_name
  from {{ref('attribute_tbl')}} m_attr
  inner join {{ref('customer_tbl')}} m_tbl on m_attr.fk_table_id=m_tbl.table_id
  where  lower(is_transform_attribute)= lower('y')
)
group by engine_name