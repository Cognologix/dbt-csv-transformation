WITH sc_elo_types AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(tx_elotypedescriptor),'') AS tx_elotypedescriptor,
		tx_begindate,
		tx_enddate

	FROM
		{{ source('raw_data', 'school_elo_types')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(tx_elotypedescriptor),'') IS NOT NULL

)

select * from sc_elo_types


