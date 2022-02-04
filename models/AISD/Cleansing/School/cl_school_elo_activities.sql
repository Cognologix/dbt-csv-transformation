WITH sc_elo_activities AS (

	SELECT
		schoolid,
		NULLIF(TRIM(tx_eloactivitydescriptor),'') AS tx_eloactivitydescriptor,
		NULLIF(TRIM(tx_elodaysscheduledperyear),'') AS tx_elodaysscheduledperyear,
		NULLIF(TRIM(tx_elominutesscheduledperday),'') AS tx_elominutesscheduledperday

	FROM
		{{source('public', 'school_elo_activities')}}

	WHERE
	    schoolid IS NOT NULL AND
        NULLIF(TRIM(tx_eloactivitydescriptor),'') IS NOT NULL
)

select * from sc_elo_activities
