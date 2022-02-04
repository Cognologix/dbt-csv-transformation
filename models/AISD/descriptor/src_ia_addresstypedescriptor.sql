WITH addresstypedescriptor as (
    select * from {{ source('dbt_test', 'addresstypedescriptor')}}
)

select * from addresstypedescriptor