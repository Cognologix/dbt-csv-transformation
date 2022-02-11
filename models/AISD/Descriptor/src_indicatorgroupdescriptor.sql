WITH indicatorgroupdescriptor as (
    select * from {{ source('dbt_test', 'indicatorgroupdescriptor')}}
)

select * from indicatorgroupdescriptor