WITH operationalstatusdescriptor as (
    select * from {{ source('dbt_test', 'operationalstatusdescriptor')}}
)

select * from operationalstatusdescriptor