WITH personalinfoverificationdescriptor as (
    select * from {{ source('dbt_test', 'personalinformationverificationdescriptor')}}
)

select * from personalinfoverificationdescriptor