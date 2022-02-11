WITH sc_gradelevels AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(gradeleveldescriptor),'') AS gradeleveldescriptor

	FROM
		{{ source('raw_data', 'school_gradelevels')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(gradeleveldescriptor),'') IS NOT NULL


)

select * from sc_gradelevels