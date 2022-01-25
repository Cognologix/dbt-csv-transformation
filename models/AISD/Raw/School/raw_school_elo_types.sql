WITH sc_elo_types AS (

	SELECT
		schoolid,
		NULLIF(TRIM(tx_elotypeDescriptor),'') AS tx_elotypeDescriptor,
		tx_begindate,
		tx_enddate

	FROM
		{{ source('public', 'school_elo_types')}}

	WHERE
	    schoolid IS NOT NULL
)

select * from sc_elo_types


