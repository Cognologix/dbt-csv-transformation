WITH st_crisis_events AS (

	SELECT
		sce.studentuniqueid,
		TRIM(sce.tx_crisiseventdescriptor) AS tx_crisiseventdescriptor,
		sce.tx_begindate,
		sce.tx_enddate

	FROM
		{{ source('public', 'student_crisis_events')}} AS sce
	WHERE
	    studentuniqueid IS NOT NULL AND
	    NULLIF(TRIM(tx_crisiseventdescriptor),'') IS NOT NULL
)

select * from st_crisis_events



