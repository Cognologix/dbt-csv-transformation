-- Apply lookup and other business transformations at this stage
WITH sc_categories as (

    SELECT
    	distinct schoolid,
		        schoolcategorydescriptor
    from
        {{ref('raw_school_categories')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_categories.schoolid,
		jsonb_agg(json_build_object(
				'schoolCategoryDescriptor', sc_categories.schoolcategorydescriptor

			)) AS schoolCategories
   FROM
	    sc_categories
   GROUP BY
		sc_categories.schoolid

)

select * from final