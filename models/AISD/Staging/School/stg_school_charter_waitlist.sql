-- Apply lookup and other business transformations at this stage
WITH sc_charter_waitlist as (

    SELECT * from {{ref('cl_school_charter_waitlist')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
        sc_charter_waitlist.loadid as LOADID,
		sc_charter_waitlist.schoolid,
		jsonb_agg(json_build_object(
				'txNumberCharterStudentsEnrolled', sc_charter_waitlist.tx_numbercharterstudentsenrolled,
				'txCharterEducationalEnrollmentCapacity', sc_charter_waitlist.tx_chartereducationalenrollmentcapacity,
                'txCharterAdmissionWaitlist', sc_charter_waitlist.tx_charteradmissionwaitlist,
				'txBeginDate', sc_charter_waitlist.tx_begindate,
                'txEndDate', sc_charter_waitlist.tx_enddate

			)) AS charterWaitlist
   FROM
	    sc_charter_waitlist

   GROUP BY
        sc_charter_waitlist.loadid,
		sc_charter_waitlist.schoolid

)

select * from final
