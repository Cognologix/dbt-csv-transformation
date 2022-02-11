WITH countrydescriptor as (
    select * from {{ source('dbt_test', 'countrydescriptor')}}
)

select * from countrydescriptor