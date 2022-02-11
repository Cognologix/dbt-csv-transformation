WITH educationorganizatio__ationsystemdescriptor as (
    select * from {{ source('dbt_test', 'educationorganizatio__ationsystemdescriptor')}}
)

select * from educationorganizatio__ationsystemdescriptor