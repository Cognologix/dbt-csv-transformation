WITH sc_itntd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_institutiontelephonenumbertypedescriptor')}} as src_itntd
   on d.descriptorid = src_itntd.institutiontelephonenumbertypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_it as (

    SELECT
        -- * from {{ref('cl_school_institution_telephones')}}
    loadid,
    schoolid,
    -- lookup Institution Telephone Number Type Descriptor use descriptor
    CASE
		-- When Institution Telephone Number Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sit.institutiontelephonenumbertypedescriptor),'') is null)
		THEN NULL

		-- When Institution Telephone Number Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sit.institutiontelephonenumbertypedescriptor),'') is not null and NULLIF(TRIM(sc_itntd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Institution Telephone Number Type Descriptor
		else concat(sc_itntd.namespace, '#', sc_itntd.codevalue)

	END as institutiontelephonenumbertypedescriptor,
	telephonenumber

    from {{ref('cl_school_institution_telephones')}} as cl_sit

    left outer join sc_itntd
    on cl_sit.institutiontelephonenumbertypedescriptor = sc_itntd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        sc_it.loadid as LOADID,
		sc_it.schoolid,
		jsonb_agg(json_build_object(
				'institutionTelephoneNumberTypeDescriptor', sc_it.institutiontelephonenumbertypedescriptor,
				'telephoneNumber', sc_it.telephonenumber

			)) AS institutionTelephones
   FROM
	    sc_it
   GROUP BY
        sc_it.loadid,
		sc_it.schoolid

)

select * from final

