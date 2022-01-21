WITH visadescriptor as (
    select * from {{ source('dbt_test', 'visadescriptor')}}
)

select * from visadescriptor;