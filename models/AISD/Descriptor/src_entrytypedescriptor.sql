WITH entrytypedescriptor as (
    select * from {{ source('dbt_test', 'entrytypedescriptor')}}
)

select * from entrytypedescriptor