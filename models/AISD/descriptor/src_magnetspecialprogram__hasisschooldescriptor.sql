WITH magnetspecialprogram__hasisschooldescriptor as (
    select * from {{ source('dbt_test', 'magnetspecialprogram__hasisschooldescriptor')}}
)

select * from magnetspecialprogram__hasisschooldescriptor