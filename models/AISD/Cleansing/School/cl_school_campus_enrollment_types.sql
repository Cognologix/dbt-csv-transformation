WITH sc_campus_enrollment_types AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(tx_campusenrollmenttypedescriptor),'') AS tx_campusenrollmenttypedescriptor,
		tx_begindate,
		tx_enddate


	FROM
		{{ source('public', 'school_campus_enrollment_types')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(tx_campusenrollmenttypedescriptor),'') IS NOT NULL

)

select * from sc_campus_enrollment_types
