WITH sc_indicators AS (

	SELECT
		schoolid,
		NULLIF(TRIM(indicatordescriptor),'') AS indicatordescriptor,
		NULLIF(TRIM(indicatorgroupdescriptor),'') AS indicatorgroupdescriptor,
		NULLIF(TRIM(indicatorleveldescriptor),'') AS indicatorleveldescriptor,
		NULLIF(TRIM(designatedby),'') AS designatedby,
		NULLIF(TRIM(indicatorvalue),'') AS indicatorvalue,
		begindate,
		enddate

	FROM
		{{ source('public', 'school_indicators')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(indicatordescriptor),'') IS NOT NULL
)

select * from sc_indicators