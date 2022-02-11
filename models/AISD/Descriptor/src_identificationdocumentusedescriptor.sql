WITH iddocusedescriptor as (
    select * from {{ source('dbt_test', 'identificationdocumentusedescriptor')}}
)

select * from iddocusedescriptor