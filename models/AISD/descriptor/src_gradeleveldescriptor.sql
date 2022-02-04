WITH gradeleveldescriptor as (
    select * from {{ source('dbt_test', 'gradeleveldescriptor')}}
)

select * from gradeleveldescriptor