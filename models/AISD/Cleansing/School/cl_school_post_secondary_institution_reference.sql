WITH sc_post_secondary_institution_reference AS (

	SELECT
		schoolid,
		postSecondaryInstitutionId

	FROM
		{{ source('public', 'school_post_secondary_institution_reference')}}

	WHERE
	    schoolid IS NOT NULL AND
	    postSecondaryInstitutionId IS NOT NULL

)

select * from sc_post_secondary_institution_reference