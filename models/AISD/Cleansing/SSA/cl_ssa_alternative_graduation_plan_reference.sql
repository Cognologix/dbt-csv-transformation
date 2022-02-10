WITH ssa_agpr AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		studentuniqueid,
		NULLIF(TRIM(alternativegraduatio__ucationorganizationid),'') AS alternativegraduatio__ucationorganizationid,
		NULLIF(TRIM(alternativegraduatio__nplantypedescriptor),'') AS alternativegraduatio__nplantypedescriptor,
		NULLIF(TRIM(alternativegraduatio___graduationschoolyear),'') AS alternativegraduatio___graduationschoolyear



	FROM
		{{ source('public', 'ssa_alternate_graduation_plan_reference')}}

	WHERE
	    schoolid IS NOT NULL AND
	    studentuniqueid IS NOT NULL AND
        NULLIF(TRIM(alternativegraduatio__ucationorganizationid),'') IS NOT NULL AND
	    NULLIF(TRIM(alternativegraduatio__nplantypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(alternativegraduatio___graduationschoolyear),'') IS NOT NULL

)

select * from ssa_agpr
