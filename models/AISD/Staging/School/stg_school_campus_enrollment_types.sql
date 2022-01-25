-- Apply lookup and other business transformations at this stage
WITH sc_cet as (

    SELECT
    	distinct schoolid,
		        tx_campusenrollmenttypedescriptor,
		        tx_begindate,
		        tx_enddate
    from
        {{ref('raw_school_campus_enrollment_types')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		sc_cet.schoolid,
		jsonb_agg(json_build_object(
				'txCampusEnrollmentTypeDescriptor', sc_cet.tx_campusenrollmenttypedescriptor,
				'txBeginDate', sc_cet.tx_begindate,
				'txEndDate', sc_cet.tx_enddate

			)) AS campusEnrollmentTypes
   FROM
	    sc_cet
   GROUP BY
		sc_cet.schoolid

)

select * from final
