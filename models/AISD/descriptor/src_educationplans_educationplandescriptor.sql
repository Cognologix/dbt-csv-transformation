WITH educationplans_educationplandescriptor as (
    select * from {{ source('dbt_test', 'educationplandescriptor')}}
)

select * from educationplans_educationplandescriptor