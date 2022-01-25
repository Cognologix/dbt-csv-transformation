WITH st_base AS (
	SELECT
		sb.studentuniqueid,
		sb.operation,
		TRIM(sb.sourcesystemdescriptor) AS sourcesystemdescriptor,
		TRIM(sb.birthcity) AS birthcity,
		TRIM(sb.birthcountrydescriptor) AS birthcountrydescriptor,
		sb.birthdate,
		TRIM(sb.birthinternationalprovince) AS birthinternationalprovince,
		TRIM(sb.birthsexdescriptor) AS birthsexdescriptor,
		TRIM(sb.birthstateabbreviationdescriptor) AS birthstateabbreviationdescriptor,
		TRIM(sb.citizenshipstatusdescriptor) AS citizenshipstatusdescriptor,
		TRIM(sb.dateenteredus) AS dateenteredus,
		TRIM(sb.firstname) AS firstname,
		TRIM(sb.generationcodesuffix) AS generationcodesuffix,
		TRIM(sb.lastsurname) AS lastsurname,
		TRIM(sb.maidenname) AS maidenname,
		TRIM(sb.middlename) AS middlename,
		TRIM(sb.multiplebirthstatus) AS multiplebirthstatus,
		TRIM(sb.personaltitleprefix) AS personaltitleprefix,
		TRIM(sb.tx_adultpreviousattendanceindicator) AS tx_adultpreviousattendanceindicator,
		sb.tx_localstudentid,
		sb.tx_studentid
    FROM
		{{ source('public', 'student_base')}} AS sb
    WHERE
	    studentuniqueid IS NOT NULL
	    AND birthdate IS NOT NULL
	    AND NULLIF(TRIM(firstname),'') IS NOT NULL
	    AND NULLIF(TRIM(lastsurname),'') IS NOT NULL
)

select * from st_base