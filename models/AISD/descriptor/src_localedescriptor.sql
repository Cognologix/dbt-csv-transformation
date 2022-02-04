WITH localedescriptor as (
    select * from {{ source('dbt_test', 'localedescriptor')}}
)

select * from localedescriptor