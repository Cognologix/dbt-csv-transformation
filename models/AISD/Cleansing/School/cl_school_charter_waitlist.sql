WITH sc_charter_waitlist AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		tx_begindate,
		tx_enddate,
		NULLIF(TRIM(tx_numbercharterstudentsenrolled),'') AS tx_numbercharterstudentsenrolled,
		NULLIF(TRIM(tx_chartereducationalenrollmentcapacity),'') AS tx_chartereducationalenrollmentcapacity,
		NULLIF(TRIM(tx_charteradmissionwaitlist),'') AS tx_charteradmissionwaitlist

    FROM
		{{ source('raw_data', 'school_charter_waitlist')}}

	WHERE
	    schoolid IS NOT NULL
	    AND tx_begindate IS NOT NULL
		AND tx_enddate IS NOT NULL

)

select * from sc_charter_waitlist


