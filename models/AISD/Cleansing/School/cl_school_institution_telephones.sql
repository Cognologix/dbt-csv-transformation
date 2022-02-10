WITH sc_institution_telephones AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(institutiontelephonenumbertypedescriptor),'') AS institutiontelephonenumbertypedescriptor,
		NULLIF(TRIM(telephonenumber),'') AS telephonenumber


	FROM
		{{ source('public', 'school_institution_telephones')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(institutiontelephonenumbertypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(telephonenumber),'') IS NOT NULL

)

select * from sc_institution_telephones

