WITH sc_epd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_educationplans_educationplandescriptor')}} as src_epd
   on d.descriptorid = src_epd.educationplandescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
s_ep as (

    SELECT
        -- * from {{ref('cl_ssa_alternative_graduation_plan_reference')}}
    loadid,
    schoolid,
    studentuniqueid,

    -- lookup Graduation Plan Descriptor use descriptor
    CASE
		-- When Alternative Graduation Plan Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_ep.educationplans_educationplandescriptor),'') is null)
		THEN NULL

		-- When Graduation Plan Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_ep.educationplans_educationplandescriptor),'') is not null and NULLIF(TRIM(sc_epd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Graduation Plan Descriptor
		else concat(sc_epd.namespace, '#', sc_epd.codevalue)

	END as educationplans_educationplandescriptor


    from {{ref('cl_ssa_education_plans')}} as cl_ep

    left outer join sc_epd
    on cl_ep.educationplans_educationplandescriptor = sc_epd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        s_ep.loadid as LOADID,
		s_ep.schoolid,
		s_ep.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationPlanDescriptor', s_ep.educationplans_educationplandescriptor

        )) AS educationPlans

   FROM
	    s_ep
   GROUP BY
        s_ep.loadid,
		s_ep.schoolid,
		s_ep.studentuniqueid


)

select * from final
