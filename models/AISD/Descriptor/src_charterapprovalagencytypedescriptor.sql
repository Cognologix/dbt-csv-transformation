WITH charterapprovalagencytypedescriptor as (
    select * from {{ source('dbt_test', 'charterapprovalagencytypedescriptor')}}
)

select * from charterapprovalagencytypedescriptor