WITH educationorganizationcategorydescriptor as (
    select * from {{ source('dbt_test', 'educationorganizationcategorydescriptor')}}
)

select * from educationorganizationcategorydescriptor