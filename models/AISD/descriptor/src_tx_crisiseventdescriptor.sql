WITH tx_crisiseventdescriptor as (
    select * from {{ source('dbt_test', 'tx_crisiseventdescriptor')}}
)

select * from tx_crisiseventdescriptor