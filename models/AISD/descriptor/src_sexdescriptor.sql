WITH sexdescriptor as (
    select * from {{ source('dbt_test', 'sexdescriptor')}}
)

select * from sexdescriptor