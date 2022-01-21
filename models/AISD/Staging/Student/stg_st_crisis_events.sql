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
				'txCrisisEventDescriptor', TRIM(sce.txcrisiseventdescriptor),
				'txBeginDate', TRIM(sce.txbegindate),
				'txEndDate', TRIM(sce.txenddate)
			)
	FROM
	  sce

)

select * from final;