WITH tx_nslptypedescriptor as (
    select * from {{ source('dbt_test', 'tx_nslptypedescriptor')}}
)

select * from tx_nslptypedescriptor