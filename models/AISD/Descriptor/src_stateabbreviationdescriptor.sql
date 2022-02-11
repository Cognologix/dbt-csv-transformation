WITH stateabbreviationdescriptor as (
    select * from {{ source('dbt_test', 'stateabbreviationdescriptor')}}
)

select * from stateabbreviationdescriptor