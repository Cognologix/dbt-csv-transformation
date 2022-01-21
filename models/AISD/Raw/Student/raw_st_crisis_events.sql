WITH st_crisis_events AS (
	SELECT
		sce.studentuniqueid,
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', sce.tx_crisiseventdescriptor,
				'txBeginDate', sce.tx_begindate,
				'txEndDate', sce.tx_enddate
			)
		) AS crisisEvents
	FROM
		{{ source('public', 'student_crisis_events')}} AS sce
	GROUP BY
		sce.studentuniqueid
)

select * from st_crisis_events;



