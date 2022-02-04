WITH magnetspecialprogramemphasisschooldescriptor as (
    select * from {{ source('dbt_test', 'magnetspecialprogramemphasisschooldescriptor')}}
)

select * from magnetspecialprogramemphasisschooldescriptor