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
				#visaDescriptor: uri://ed-fi.org/VisaDescriptor#other visa
			)
		)
   FROM
	    sv

   JOIN  dv
	on sv.visadescriptor = concat (dv.namespace, '#', dv.codevalue)

   GROUP BY
		sv.studentuniqueid
)

select * from final;