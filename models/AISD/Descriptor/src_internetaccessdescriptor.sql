WITH internetaccessdescriptor as (
    select * from {{ source('dbt_test', 'internetaccessdescriptor')}}
)

select * from internetaccessdescriptor