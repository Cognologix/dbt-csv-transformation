WITH exitwithdrawtypedescriptor as (
    select * from {{ source('dbt_test', 'exitwithdrawtypedescriptor')}}
)

select * from exitwithdrawtypedescriptor