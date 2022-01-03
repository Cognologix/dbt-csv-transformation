
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with aisd_schools as (

    select * from aisd_schools

),
aisd_local_education_agency as (
   select * from aisd_local_education_agency
)

select school.localeducationagency__id, alea.*
from aisd_schools school , aisd_local_education_agency alea
where school.localeducationagency__id=alea.id

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
