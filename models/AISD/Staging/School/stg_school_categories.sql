WITH sc_scd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_schoolcategorydescriptor')}} as src_scd
   on d.descriptorid = src_scd.schoolcategorydescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_categories as (

    SELECT
        -- * from {{ref('cl_school_categories')}}
    schoolid,
    -- lookup School Category Descriptor use descriptor
    CASE
		-- When School Category Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sc.schoolcategorydescriptor),'') is null)
		THEN NULL

		-- When School Category Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sc.schoolcategorydescriptor),'') is not null and NULLIF(TRIM(sc_scd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new School Category Descriptor
		else concat(sc_scd.namespace, '#', sc_scd.codevalue)

	END as schoolcategorydescriptor

    from {{ref('cl_school_categories')}} as cl_sc

    left outer join sc_scd
    on cl_sc.schoolcategorydescriptor = sc_scd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
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