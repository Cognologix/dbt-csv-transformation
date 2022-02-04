WITH tx_elotypedescriptor as (
    select * from {{ source('dbt_test', 'tx_elotypedescriptor')}}
)

select * from tx_elotypedescriptor