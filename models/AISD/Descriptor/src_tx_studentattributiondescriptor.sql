WITH tx_studentattributiondescriptor as (
    select * from {{ source('dbt_test', 'tx_studentattributiondescriptor')}}
)

select * from tx_studentattributiondescriptor