WITH tx_eloactivitydescriptor as (
    select * from {{ source('dbt_test', 'tx_eloactivitydescriptor')}}
)

select * from tx_eloactivitydescriptor