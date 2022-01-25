-- Apply lookup and other business transformations at this stage
WITH s_ep as (

    SELECT
    	distinct schoolid,
    	        studentuniqueid,
		        educationplans_educationplandescriptor

    from
        {{ref('raw_ssa_education_plans')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		s_ep.schoolid,
		s_ep.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationPlanDescriptor', s_ep.educationplans_educationplandescriptor
				)
        ) AS educationPlans

   FROM
	    s_ep
   GROUP BY
		s_ep.schoolid,
		s_ep.studentuniqueid


)

select * from final
