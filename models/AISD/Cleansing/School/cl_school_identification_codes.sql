WITH sc_identification_codes AS (

	SELECT
		schoolid,
		NULLIF(TRIM(eo_identificationsystemdescriptor),'') AS eo_identificationsystemdescriptor,
		NULLIF(TRIM(identificationcode),'') AS identificationcode


	FROM
		{{ source('public', 'school_identification_codes')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(eo_identificationsystemdescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(identificationcode),'') IS NOT NULL

)

select * from sc_identification_codes