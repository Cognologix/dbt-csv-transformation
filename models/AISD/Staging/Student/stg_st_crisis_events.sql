WITH sce as (
    select * from {{ref('raw_st_crisis_events')}}
    WHERE
	sce.studentuniqueid IS NOT NULL
	AND sce.txcrisiseventdescriptor IS NOT NULL
),
final as (
    SELECT
		TRIM(sce.studentuniqueid),
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', TRIM(sce.tx_crisiseventdescriptor),
				'txBeginDate', TRIM(sce.tx_begindate),
				'txEndDate', TRIM(sce.tx_enddate)
			)
		)
	FROM
	    sce
        GROUP BY
		sce.studentuniqueid
)

select * from final;