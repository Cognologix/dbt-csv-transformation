WITH sc_txcntd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_eloactivitydescriptor')}} as src_txcntd
   on d.descriptorid = src_txcntd.tx_eloactivitydescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_elo_a as (

    SELECT
        -- * from {{ref('cl_school_campus_enrollment_types')}}
    schoolid,
    -- lookup tx_elo Activity Descriptor use descriptor
    CASE
		-- When tx_elo Activity Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sea.tx_eloactivitydescriptor),'') is null)
		THEN NULL

		-- When tx_elo Activity Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sea.tx_eloactivitydescriptor),'') is not null and NULLIF(TRIM(sc_txcntd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new tx_elo Activity Descriptor
		else concat(sc_txcntd.namespace, '#', sc_txcntd.codevalue)

	END as tx_eloactivitydescriptor,
	tx_elodaysscheduledperyear,
	tx_elominutesscheduledperday


    from {{ref('cl_school_elo_activities')}} as cl_sea

    left outer join sc_txcntd
    on cl_sea.tx_eloactivitydescriptor = sc_txcntd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
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
