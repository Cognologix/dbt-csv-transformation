-- Apply lookup and other business transformations at this stage
WITH sc_elo_a as (

    SELECT
    	distinct schoolid,
		        tx_eloactivitydescriptor,
		        tx_elodaysscheduledperyear,
		        tx_elominutesscheduledperday
    from
        {{ref('raw_school_elo_activities')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_elo_a.schoolid,
		jsonb_agg(json_build_object(
				'txELOActivityDescriptor', sc_elo_a.tx_eloactivitydescriptor,
				'txELODaysScheduledPerYear', sc_elo_a.tx_elodaysscheduledperyear,
				'txELOMinutesScheduledPerDay', sc_elo_a.tx_elominutesscheduledperday

			)) AS eloActivities
   FROM
	    sc_elo_a
   GROUP BY
		sc_elo_a.schoolid

)

select * from final
