WITH sc_eosd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_educationorganizatio__ationsystemdescriptor')}} as src_eosd
   on d.descriptorid = src_eosd.educationorganizatio__ationsystemdescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_ic as (

    SELECT
        -- * from {{ref('cl_school_identification_codes')}}
    loadid,
    schoolid,
    -- lookup Education Organization System Descriptor use descriptor
    CASE
		-- When educationorganizationidentificationsystemdescriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sic.educationorganizatio__ationsystemdescriptor),'') is null)
		THEN NULL

		-- When educationorganizationidentificationsystemdescriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sic.educationorganizatio__ationsystemdescriptor),'') is not null and NULLIF(TRIM(sc_eosd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new educationorganizationidentificationsystemdescriptor
		else concat(sc_eosd.namespace, '#', sc_eosd.codevalue)

	END as educationorganizatio__ationsystemdescriptor,
	identificationcode

    from {{ref('cl_school_identification_codes')}} as cl_sic

    left outer join sc_eosd
    on cl_sic.educationorganizatio__ationsystemdescriptor = sc_eosd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        sc_ic.loadid as LOADID,
		sc_ic.schoolid,
		jsonb_agg(json_build_object(
				'educationorganizatio__ationsystemdescriptor', sc_ic.educationorganizatio__ationsystemdescriptor,
				'identificationCode', sc_ic.identificationcode

			)) AS identificationCodes
   FROM
	    sc_ic
   GROUP BY
        sc_ic.loadid,
		sc_ic.schoolid

)

select * from final

