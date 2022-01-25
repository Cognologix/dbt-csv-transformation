WITH sourcesystemdescriptor as (
    select * from {{ source('dbt_test', 'sourcesystemdescriptor')}}
)

select * from sourcesystemdescriptor