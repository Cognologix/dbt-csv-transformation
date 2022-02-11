WITH ssa_b AS (
	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		studentuniqueid,
		NULLIF(TRIM(operation),'') AS operation,
        entrydate,
		calendarcode,
		calendarreference_schoolid,
		calendarreference_schoolyear,
		classofschoolyeartypereference_schoolyear,
		graduationplanrefere__ucationorganizationid,
		NULLIF(TRIM(graduationplanrefere__ionplantypedescriptor),'') AS graduationplanrefere__ionplantypedescriptor,
		graduationplanrefere___graduationschoolyear,
		schoolyeartypereference_schoolyear,
		NULLIF(TRIM(employedwhileenrolled),'') AS employedwhileenrolled,
		NULLIF(TRIM(entrygradeleveldescriptor),'') AS entrygradeleveldescriptor,
		NULLIF(TRIM(entrygradelevelreasondescriptor),'') AS entrygradelevelreasondescriptor,
		NULLIF(TRIM(entrytypedescriptor),'') AS entrytypedescriptor,
		exitwithdrawdate,
		NULLIF(TRIM(exitwithdrawtypedescriptor),'') AS exitwithdrawtypedescriptor,
		NULLIF(TRIM(fulltimeequivalency),'') AS fulltimeequivalency,
		NULLIF(TRIM(primaryschool),'') AS primaryschool,
		NULLIF(TRIM(repeatgradeindicator),'') AS repeatgradeindicator,
		NULLIF(TRIM(residencystatusdescriptor),'') AS residencystatusdescriptor,
		NULLIF(TRIM(schoolchoicetransfer),'') AS schoolchoicetransfer,
		NULLIF(TRIM(termcompletionindicator),'') AS termcompletionindicator,
		NULLIF(TRIM(tx_adaeligibilitydescriptor),'') AS tx_adaeligibilitydescriptor,
		NULLIF(TRIM(tx_studentattributiondescriptor),'') AS tx_studentattributiondescriptor

	FROM
		{{ source('raw_data', 'ssa_base')}}

	WHERE
	    schoolid IS NOT NULL AND
        studentuniqueid IS NOT NULL AND
        entrydate IS NOT NULL AND
        NULLIF(TRIM(entrygradeleveldescriptor),'') IS NOT NULL
)

select * from ssa_b

