WITH tx_campusenrollmenttypedescriptor as (
    select * from {{ source('dbt_test', 'tx_campusenrollmenttypedescriptor')}}
)

select * from tx_campusenrollmenttypedescriptor