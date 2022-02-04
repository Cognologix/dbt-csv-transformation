WITH sc_eocd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_educationorganizationcategorydescriptor')}} as src_eocd
   on d.descriptorid = src_eocd.educationorganizationcategorydescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_eoc as (

    SELECT
        -- * from {{ref('cl_school_education_organization_categories')}}
    schoolid,
    -- lookup Education Organization Category Descriptor use descriptor
    CASE
		-- When Education Organization Category Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_seoc.educationorganizationcategorydescriptor),'') is null)
		THEN NULL

		-- When Education Organization Category Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_seoc.educationorganizationcategorydescriptor),'') is not null and NULLIF(TRIM(sc_eocd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Education Organization Category Descriptor
		else concat(sc_eocd.namespace, '#', sc_eocd.codevalue)

	END as educationorganizationcategorydescriptor

    from {{ref('cl_school_education_organization_categories')}} as cl_seoc

    left outer join sc_eocd
    on cl_seoc.educationorganizationcategorydescriptor = sc_eocd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
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