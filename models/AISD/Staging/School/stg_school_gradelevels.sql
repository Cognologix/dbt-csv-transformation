-- Apply lookup and other business transformations at this stage
WITH sc_gl as (

    SELECT
    	distinct schoolid,
		        gradeleveldescriptor
    from
        {{ref('raw_school_gradelevels')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_gl.schoolid,
		jsonb_agg(json_build_object(
				'gradeLevelDescriptor', sc_gl.gradeleveldescriptor

        )) AS gradeLevels
   FROM
	    sc_gl
   GROUP BY
		sc_gl.schoolid

)

select * from final
