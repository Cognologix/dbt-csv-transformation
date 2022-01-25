WITH ssa_agpr AS (

	SELECT
		schoolid,
		studentuniqueid,
		NULLIF(TRIM(alternativegraduatio__ucationorganizationid),'') AS alternativegraduatio__ucationorganizationid,
		NULLIF(TRIM("alternativeGraduatio__nPlanTypeDescriptor.1"),'') AS "alternativeGraduatio__nPlanTypeDescriptor.1",
		NULLIF(TRIM(alternativegraduatio___graduationschoolyear),'') AS alternativegraduatio___graduationschoolyear



	FROM
		{{ source('public', 'ssa_alternate_graduation_plan_reference')}}

	WHERE
	    schoolid IS NOT NULL AND
	    studentuniqueid IS NOT NULL AND
        NULLIF(TRIM(alternativegraduatio__ucationorganizationid),'') IS NOT NULL AND
	    NULLIF(TRIM("alternativeGraduatio__nPlanTypeDescriptor.1"),'') IS NOT NULL AND
	    NULLIF(TRIM(alternativegraduatio___graduationschoolyear),'') IS NOT NULL

)

select * from ssa_agpr
