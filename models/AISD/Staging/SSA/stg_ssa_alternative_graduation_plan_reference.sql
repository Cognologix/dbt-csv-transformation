WITH ssa_agptd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_alternativegraduatio__nplantypedescriptor')}} as src_agptd
   on d.descriptorid = src_agptd.graduationplantypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
s_agpr as (

    SELECT
        -- * from {{ref('cl_ssa_alternative_graduation_plan_reference')}}
    loadid,
    schoolid,
    studentuniqueid,
    alternativegraduatio__ucationorganizationid,

    -- lookup Alternative Graduation Plan Type Descriptor use descriptor
    CASE
		-- When Alternative Graduation Plan Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_agpr.alternativegraduatio__nplantypedescriptor),'') is null)
		THEN NULL

		-- When Alternative Graduation Plan Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_agpr.alternativegraduatio__nplantypedescriptor),'') is not null and NULLIF(TRIM(ssa_agptd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Alternative Graduation Plan Type Descriptor
		else concat(ssa_agptd.namespace, '#', ssa_agptd.codevalue)

	END as alternativegraduatio__nplantypedescriptor,
	alternativegraduatio___graduationschoolyear


    from {{ref('cl_ssa_alternative_graduation_plan_reference')}} as cl_agpr

    left outer join ssa_agptd
    on cl_agpr.alternativegraduatio__nplantypedescriptor = ssa_agptd.codevalue
),

------------------------------------------------------------------------------
-- nested Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------




------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------

final as (
   SELECT
        s_agpr.loadid as LOADID,
		s_agpr.schoolid,
		s_agpr.studentuniqueid,
		jsonb_agg(json_build_object('alternativeGraduationPlanReference',
		json_build_object(
				'educationOrganizationId', s_agpr.alternativegraduatio__ucationorganizationid,
                'graduationPlanTypeDescriptor', s_agpr.alternativegraduatio__nplantypedescriptor,
				'graduationSchoolYear', s_agpr.alternativegraduatio___graduationschoolyear


        ))) AS alternativeGraduationPlans
   FROM
	    s_agpr
   GROUP BY
        s_agpr.loadid,
		s_agpr.schoolid,
		s_agpr.studentuniqueid


)

select * from final


