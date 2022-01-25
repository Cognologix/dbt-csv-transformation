-- Apply lookup and other business transformations at this stage
WITH sc_eoc as (

    SELECT
    	distinct schoolid,
		        educationorganizationcategorydescriptor
    from
        {{ref('raw_school_education_organization_categories')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_eoc.schoolid,
		jsonb_agg(json_build_object(
				'educationOrganizationCategoryDescriptor', sc_eoc.educationorganizationcategorydescriptor

			)) AS educationOrganizationCategories
   FROM
	    sc_eoc
   GROUP BY
		sc_eoc.schoolid

)

select * from final