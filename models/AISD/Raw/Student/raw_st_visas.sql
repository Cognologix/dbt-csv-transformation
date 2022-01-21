WITH st_visas AS (
	SELECT
		sv.studentuniqueid,
		jsonb_agg(json_build_object(
				'visaDescriptor', sv.visadescriptor
			)
		) AS visas
	FROM
		{{ source('public', '_airbyte_raw_student_visas')}} AS sv
	GROUP BY
		sv.studentuniqueid
)

select * from st_visas;