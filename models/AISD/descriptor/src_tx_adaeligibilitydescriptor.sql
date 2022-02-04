WITH tx_adaeligibilitydescriptor as (
    select * from {{ source('dbt_test', 'tx_adaeligibilitydescriptor')}}
)

select * from tx_adaeligibilitydescriptor