WITH st_base AS (
	SELECT
		sb.studentuniqueid,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'studentuniqueid', sb.studentuniqueid
		)AS externalid,
		'STUDENT' AS resourcetype,
		sb.operation,
		0 AS status,
		json_build_object(
			'personId', sb.studentuniqueid,
			'sourceSystemDescriptor', sb.sourcesystemdescriptor
		) AS identificationDocuments,
		sb.birthcity,
		sb.birthcountrydescriptor,
		sb.birthdate,
		sb.birthinternationalprovince,
		sb.birthsexdescriptor,
		sb.birthstateabbreviationdescriptor,
		sb.citizenshipstatusdescriptor,
		sb.dateenteredus,
		sb.firstname,
		sb.generationcodesuffix,
		sb.lastsurname,
		sb.maidenname,
		sb.middlename,
		sb.multiplebirthstatus,
		sb.personaltitleprefix,
		sb.tx_adultpreviousattendanceindicator,
		sb.tx_localstudentid,
		sb.tx_studentid

	FROM
		{{ source('public', '_airbyte_raw_student_base')}} AS sb
)

select * from st_base;