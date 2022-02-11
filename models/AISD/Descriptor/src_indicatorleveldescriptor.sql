WITH indicatorleveldescriptor as (
    select * from {{ source('dbt_test', 'indicatorleveldescriptor')}}
)

select * from indicatorleveldescriptor