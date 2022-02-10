WITH ssa_ep AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		studentuniqueid,
		NULLIF(TRIM(educationplans_educationplandescriptor),'') AS educationplans_educationplandescriptor


	FROM
		{{ source('public', 'ssa_education_plans')}}

	WHERE
	    schoolid IS NOT NULL AND
	    studentuniqueid IS NOT NULL AND
	    NULLIF(TRIM(educationplans_educationplandescriptor),'') IS NOT NULL

)

select * from ssa_ep

