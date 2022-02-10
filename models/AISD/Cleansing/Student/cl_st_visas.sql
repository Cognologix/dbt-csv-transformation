WITH st_visas AS (
	SELECT
		{{ var('LOADID',-1) }} as LOADID,
		sv.studentuniqueid,
		TRIM(sv.visadescriptor) AS visadescriptor

	FROM
		{{ source('raw_data', 'student_visas')}} AS sv
    WHERE
        studentuniqueid IS NOT NULL AND
	    nullif(TRIM(visadescriptor),'') IS NOT NULL
)

select * from st_visas