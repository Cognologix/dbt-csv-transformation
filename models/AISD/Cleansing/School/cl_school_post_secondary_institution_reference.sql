WITH sc_post_secondary_institution_reference AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		postsecondaryinstitutionid

	FROM
		{{ source('public', 'school_post_secondary_institution_reference')}}

	WHERE
	    schoolid IS NOT NULL AND
	    postsecondaryinstitutionid IS NOT NULL

)

select * from sc_post_secondary_institution_reference