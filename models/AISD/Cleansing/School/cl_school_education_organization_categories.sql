WITH sc_education_organization_categories AS (

	SELECT
		{{ var('LOADID',-1) }} as LOADID,
        schoolid,
		NULLIF(TRIM(educationorganizationcategorydescriptor),'') AS educationorganizationcategorydescriptor
    FROM
		{{ source('public', 'school_education_organization_category')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(educationorganizationcategorydescriptor),'') IS NOT NULL
)

select * from sc_education_organization_categories