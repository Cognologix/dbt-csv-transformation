WITH ssa_agpr AS (

	SELECT
		schoolid,
		studentuniqueid,
		NULLIF(TRIM(agpr_educationorganizationid),'') AS agpr_educationorganizationid,
		NULLIF(TRIM(agpr_graduationplantypedescriptor),'') AS agpr_graduationplantypedescriptor,
		NULLIF(TRIM(agpr_graduationschoolyear),'') AS agpr_graduationschoolyear



	FROM
		{{ source('public', 'ssa_alternate_graduation_plan_reference')}}

	WHERE
	    schoolid IS NOT NULL AND
	    studentuniqueid IS NOT NULL AND
        NULLIF(TRIM(agpr_educationorganizationid),'') IS NOT NULL AND
	    NULLIF(TRIM(agpr_graduationplantypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(agpr_graduationschoolyear),'') IS NOT NULL

)

select * from ssa_agpr
