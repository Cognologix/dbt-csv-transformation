WITH sc_nslp_types AS (

	SELECT
		schoolid,
		NULLIF(TRIM(tx_nslptypedescriptor),'') AS tx_nslptypedescriptor,
		tx_begindate,
		tx_enddate


	FROM
		{{ source('public', 'school_nslp_types')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(tx_nslptypedescriptor),'') IS NOT NULL
)

select * from sc_nslp_types


