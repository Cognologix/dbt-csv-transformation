WITH eo_identificationsystemdescriptor as (
    select * from {{ source('dbt_test', 'eo_identificationsystemdescriptor')}}
)

select * from eo_identificationsystemdescriptor