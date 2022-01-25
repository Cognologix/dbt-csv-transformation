WITH othernametypedescriptor as (
    select * from {{ source('dbt_test', 'othernametypedescriptor')}}
)

select * from othernametypedescriptor