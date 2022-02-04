WITH institutiontelephonenumbertypedescriptor as (
    select * from {{ source('dbt_test', 'institutiontelephonenumbertypedescriptor')}}
)

select * from institutiontelephonenumbertypedescriptor