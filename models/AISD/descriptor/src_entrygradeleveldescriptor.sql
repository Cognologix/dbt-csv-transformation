WITH entrygradeleveldescriptor as (
    select * from {{ source('dbt_test', 'gradeleveldescriptor')}}
)

select * from entrygradeleveldescriptor