WITH st_visas AS (
	SELECT
		sv.studentuniqueid,
		TRIM(sv.visadescriptor) AS visadescriptor

	FROM
		{{ source('public', 'student_visas')}} AS sv
    WHERE
        studentuniqueid IS NOT NULL AND
	    nullif(TRIM(visadescriptor),'') IS NOT NULL
)

select * from st_visas