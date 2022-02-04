WITH agpr_graduationplantypedescriptor as (
    select * from {{ source('dbt_test', 'graduationplantypedescriptor')}}
)

select * from agpr_graduationplantypedescriptor