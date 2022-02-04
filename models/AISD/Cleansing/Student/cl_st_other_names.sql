WITH st_other_names AS (

	SELECT
		studentuniqueid,
		NULLIF(TRIM(othernametypedescriptor),'') AS othernametypedescriptor,
		NULLIF(TRIM(firstname),'') AS firstname,
		NULLIF(TRIM(generationcodesuffix),'') AS generationcodesuffix,
		NULLIF(TRIM(lastsurname),'') AS lastsurname,
		NULLIF(TRIM(middlename),'') AS middlename,
		NULLIF(TRIM(personaltitleprefix),'') AS personaltitleprefix

	FROM
		{{ source('public', 'student_other_names')}}

	WHERE
	    studentuniqueid IS NOT NULL AND
	    NULLIF(TRIM(othernametypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(firstname),'') IS NOT NULL

)

select * from st_other_names