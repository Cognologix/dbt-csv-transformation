WITH charterstatusdescriptor as (
    select * from {{ source('dbt_test', 'charterstatusdescriptor')}}
)

select * from charterstatusdescriptor