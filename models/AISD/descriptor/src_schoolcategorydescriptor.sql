WITH schoolcategorydescriptor as (
    select * from {{ source('dbt_test', 'schoolcategorydescriptor')}}
)

select * from schoolcategorydescriptor