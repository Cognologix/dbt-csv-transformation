WITH sc_identification_codes AS (

	SELECT
		schoolid,
		NULLIF(TRIM(educationorganizatio__ationsystemdescriptor),'') AS educationorganizatio__ationsystemdescriptor,
		NULLIF(TRIM(identificationcode),'') AS identificationcode


	FROM
		{{ source('public', 'school_identification_codes')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(educationorganizatio__ationsystemdescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(identificationcode),'') IS NOT NULL

)

select * from sc_identification_codes