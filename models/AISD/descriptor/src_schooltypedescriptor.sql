WITH schooltypedescriptor as (
    select * from {{ source('dbt_test', 'schooltypedescriptor')}}
)

select * from schooltypedescriptor