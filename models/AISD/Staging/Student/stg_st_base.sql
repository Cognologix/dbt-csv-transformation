WITH sb as (
    select * from {{ref('raw_st_base')}}
    WHERE
	    sb.studentuniqueid IS NOT NULL
	    AND sb.birthdate IS NOT NULL
	    AND sb.firstname IS NOT NULL
	    AND sb.lastsurname IS NOT NULL
),

final as (
    SELECT
		TRIM(sb.studentuniqueid,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'studentuniqueid', TRIM(sb.studentuniqueid)
		)AS externalid,
		'STUDENT' AS resourcetype,
		TRIM(sb.operation),
		0 AS status,
		json_build_object(
			'personId', TRIM(sb.studentuniqueid),
			'sourceSystemDescriptor', TRIM(sb.sourcesystemdescriptor)
		) AS identificationDocuments,
		TRIM(sb.birthcity),
		TRIM(sb.birthcountrydescriptor),
		TRIM(sb.birthdate),
		TRIM(sb.birthinternationalprovince),
		TRIM(sb.birthsexdescriptor),
		TRIM(sb.birthstateabbreviationdescriptor),
		TRIM(sb.citizenshipstatusdescriptor),
		TRIM(sb.dateenteredus),
		TRIM(sb.firstname),
		TRIM(sb.generationcodesuffix),
		TRIM(sb.lastsurname),
		TRIM(sb.maidenname),
		TRIM(sb.middlename),
		TRIM(sb.multiplebirthstatus),
		TRIM(sb.personaltitleprefix),
		TRIM(sb.tx_adultpreviousattendanceindicator),
		TRIM(sb.tx_localstudentid),
		TRIM(sb.tx_studentid)

	FROM
		sb

)

select * from final;
