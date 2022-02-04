WITH gpr_graduationplantypedescriptor as (
    select * from {{ source('dbt_test', 'graduationplantypedescriptor')}}
)

select * from gpr_graduationplantypedescriptor