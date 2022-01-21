WITH st_crisis_events AS (
	SELECT
		sce.studentuniqueid,
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', sce.txcrisiseventdescriptor,
				'txBeginDate', sce.txbegindate,
				'txEndDate', sce.txenddate
			)
		) AS crisisEvents
	FROM
		{{ source('public', '_airbyte_raw_student_crisis_events')}} AS sce
	GROUP BY
		sce.studentuniqueid
)

select * from st_crisis_events;



