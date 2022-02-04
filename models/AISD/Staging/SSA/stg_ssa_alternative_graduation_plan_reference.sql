WITH ssa_agptd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_agpr_graduationplantypedescriptor')}} as src_agptd
   on d.descriptorid = src_agptd.graduationplantypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
s_agpr as (

    SELECT
        -- * from {{ref('cl_ssa_alternative_graduation_plan_reference')}}
    schoolid,
    studentuniqueid,
    agpr_educationorganizationid,

    -- lookup Alternative Graduation Plan Type Descriptor use descriptor
    CASE
		-- When Alternative Graduation Plan Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_agpr.agpr_graduationplantypedescriptor),'') is null)
		THEN NULL

		-- When Alternative Graduation Plan Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_agpr.agpr_graduationplantypedescriptor),'') is not null and NULLIF(TRIM(ssa_agptd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Alternative Graduation Plan Type Descriptor
		else concat(ssa_agptd.namespace, '#', ssa_agptd.codevalue)

	END as agpr_graduationplantypedescriptor,
	agpr_graduationschoolyear


    from {{ref('cl_ssa_alternative_graduation_plan_reference')}} as cl_agpr

    left outer join ssa_agptd
    on cl_agpr.agpr_graduationplantypedescriptor = ssa_agptd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
		s_agpr.schoolid,
		s_agpr.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationOrganizationId', s_agpr.agpr_educationorganizationid,
                'graduationPlanTypeDescriptor', s_agpr.agpr_graduationplantypedescriptor,
				'graduationSchoolYear', s_agpr.agpr_graduationschoolyear


        )) AS alternativeGraduationPlans
   FROM
	    s_agpr
   GROUP BY
		s_agpr.schoolid,
		s_agpr.studentuniqueid


)

select * from final


