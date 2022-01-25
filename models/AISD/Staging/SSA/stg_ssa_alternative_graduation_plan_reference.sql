-- Apply lookup and other business transformations at this stage
WITH s_agpr as (

    SELECT
    	distinct schoolid,
    	        studentuniqueid,
		        alternativegraduatio__ucationorganizationid,
		        "alternativeGraduatio__nPlanTypeDescriptor.1",
		        alternativegraduatio___graduationschoolyear

    from
        {{ref('raw_ssa_alternative_graduation_plan_reference')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		s_agpr.schoolid,
		s_agpr.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationOrganizationId', s_agpr.alternativegraduatio__ucationorganizationid,
                'graduationPlanTypeDescriptor', s_agpr."alternativeGraduatio__nPlanTypeDescriptor.1",
				'graduationSchoolYear', s_agpr.alternativegraduatio___graduationschoolyear

        )) AS alternativeGraduationPlans
   FROM
	    s_agpr
   GROUP BY
		s_agpr.schoolid,
		s_agpr.studentuniqueid


)

select * from final


