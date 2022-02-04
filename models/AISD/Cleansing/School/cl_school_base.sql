WITH school_b AS (

	SELECT
		schoolid,
		NULLIF(TRIM(operation),'') AS operation,
		NULLIF(TRIM(administrativefundingcontroldescriptor),'') AS administrativefundingcontroldescriptor,
		NULLIF(TRIM(charterapprovalagencytypedescriptor),'') charterapprovalagencytypedescriptor,
		charterapprovalschoolyeartypereference_schoolyear,
		localeducationagencyid,
		NULLIF(TRIM(charterstatusdescriptor),'') AS charterstatusdescriptor,
		NULLIF(TRIM(internetaccessdescriptor),'') AS internetaccessdescriptor,
		NULLIF(TRIM(magnetspecialprogramemphasisschooldescriptor),'') AS magnetspecialprogramemphasisschooldescriptor,
		NULLIF(TRIM(nameofinstitution),'') AS nameofinstitution,
		NULLIF(TRIM(operationalstatusdescriptor),'') AS operationalstatusdescriptor,
		NULLIF(TRIM(schooltypedescriptor),'') AS schooltypedescriptor,
		NULLIF(TRIM(shortnameofinstitution),'') AS shortnameofinstitution,
		NULLIF(TRIM(titleipartaschooldesignationdescriptor),'') AS titleipartaschooldesignationdescriptor,
		NULLIF(TRIM(website),'') AS website,
		NULLIF(TRIM(tx_pkfulldaywaiver),'') AS tx_pkfulldaywaiver,
		NULLIF(TRIM(tx_additionaldaysprogram),'') AS  tx_additionaldaysprogram,
		tx_numberofbullyingincidents AS tx_numberofbullyingincidents,
		tx_numberofcyberbullyingincidents AS tx_numberofcyberbullyingincidents

	FROM
		{{ source('public', 'school_base')}}

	WHERE
	    schoolid IS NOT NULL AND
        NULLIF(TRIM(nameofinstitution),'') IS NOT NULL
)

select * from school_b

