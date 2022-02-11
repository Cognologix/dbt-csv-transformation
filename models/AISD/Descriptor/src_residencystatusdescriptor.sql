WITH residencystatusdescriptor as (
    select * from {{ source('dbt_test', 'residencystatusdescriptor')}}
)

select * from residencystatusdescriptor