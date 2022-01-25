WITH sc_categories AS (

	SELECT
		schoolid,
		NULLIF(TRIM(schoolcategorydescriptor),'') AS schoolcategorydescriptor
    FROM
		{{ source('public', 'school_categories')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(schoolcategorydescriptor),'') IS NOT NULL
)

select * from sc_categories