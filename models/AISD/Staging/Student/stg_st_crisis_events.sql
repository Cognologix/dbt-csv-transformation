-- Add lookup and other business validations at this stage
WITH sce as (

    SELECT * from {{ref('cl_st_crisis_events')}}

),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sce.studentuniqueid,
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', sce.tx_crisiseventdescriptor,
				'txBeginDate', sce.tx_begindate,
				'txEndDate', sce.tx_enddate
			)) as crisisEvents
	FROM
	  sce
    GROUP BY
		sce.studentuniqueid
)

select * from final