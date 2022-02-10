WITH sc_gradelevels AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(gradeleveldescriptor),'') AS gradeleveldescriptor

	FROM
		{{ source('public', 'school_gradelevels')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(gradeleveldescriptor),'') IS NOT NULL


)

select * from sc_gradelevels