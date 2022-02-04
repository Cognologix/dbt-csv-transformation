WITH titleipartaschooldesignationdescriptor as (
    select * from {{ source('dbt_test', 'titleipartaschooldesignationdescriptor')}}
)

select * from titleipartaschooldesignationdescriptor