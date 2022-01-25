-- Add lookup and other business validations at this stage
WITH sb as (
    select * from {{ref('raw_st_base')}}

),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sb.studentuniqueid,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'studentuniqueid', sb.studentuniqueid
		) AS externalid,
		'STUDENT' AS resourcetype,
		TRIM(sb.operation) AS operation,
		0 AS status,
		json_build_object(
			'personId', sb.studentuniqueid,
			'sourceSystemDescriptor', sb.sourcesystemdescriptor
		) AS personReference,
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
		sb

)

select * from final
