WITH alternativegraduatio__nplantypedescriptor as (
    select * from {{ source('dbt_test', 'graduationplantypedescriptor')}}
)

select * from alternativegraduatio__nplantypedescriptor