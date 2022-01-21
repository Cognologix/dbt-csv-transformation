WITH descriptor as (
    select * from {{ source('dbt_test', 'descriptor')}}
)

select * from descriptor;