WITH citizenshipstatusdescriptor as (
    select * from {{ source('dbt_test', 'citizenshipstatusdescriptor')}}
)

select * from citizenshipstatusdescriptor