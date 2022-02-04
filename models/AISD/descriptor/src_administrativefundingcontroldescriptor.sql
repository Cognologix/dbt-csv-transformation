WITH administrativefundingcontroldescriptor as (
    select * from {{ source('dbt_test', 'administrativefundingcontroldescriptor')}}
)

select * from administrativefundingcontroldescriptor