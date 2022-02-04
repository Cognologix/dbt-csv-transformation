WITH calendartypedescriptor as (
    select * from {{ source('dbt_test', 'calendartypedescriptor')}}
)

select * from calendartypedescriptor