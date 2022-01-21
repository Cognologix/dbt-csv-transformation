WITH sv as (
    select * from {{ref('raw_st_visas')}}
    WHERE
        sv.studentuniqueid IS NOT NULL
	    AND sv.visadescriptor IS NOT NULL
),
final as (
   SELECT
		TRIM(sv.studentuniqueid),
		jsonb_agg(json_build_object(
				'visaDescriptor', TRIM(sv.visadescriptor)
			)
   FROM
	    sv


)

select * from final;