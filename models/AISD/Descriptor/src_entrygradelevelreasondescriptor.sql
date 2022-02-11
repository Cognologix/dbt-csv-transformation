WITH entrygradelevelreasondescriptor as (
    select * from {{ source('dbt_test', 'entrygradelevelreasondescriptor')}}
)

select * from entrygradelevelreasondescriptor