WITH indicatordescriptor as (
    select * from {{ source('dbt_test', 'indicatordescriptor')}}
)

select * from indicatordescriptor