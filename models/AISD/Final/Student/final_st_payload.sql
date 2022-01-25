WITH  final as (

SELECT
	sb.resourceid,
	sb.externalid,
	sb.resourcetype,
	sb.operation,
	json_build_object(
		'studentUniqueId', sb.studentuniqueid,
		'personReference', sb.personReference,
		'birthCity', sb.birthcity,
		'birthCountryDescriptor', sb.birthcountrydescriptor,
		'birthDate', sb.birthdate,
		'birthInternationalProvince', sb.birthinternationalprovince,
		'birthSexDescriptor', sb.birthsexdescriptor,
		'birthStateAbbreviationDescriptor', sb.birthstateabbreviationdescriptor,
		'citizenshipStatusDescriptor', sb.citizenshipstatusdescriptor,
		'dateEnteredUS', sb.dateenteredus,
		'firstName', sb.firstname,
		'generationCodeSuffix', sb.generationcodesuffix,
		'identificationDocuments', sid.identificationDocuments,
		'lastSurname', sb.lastSurname,
		'maidenName', sb.maidenName,
		'middleName', sb.middleName,
		'multiplebirthstatus', sb.multiplebirthstatus,
		'otherNames', son.othernames,
		'personalIdentificationDocuments', spid.personalIdentificationDocuments,
		'personalTitlePrefix', sb.personalTitlePrefix,
		'visas', sv.visas
	) AS payload,
	status
FROM
	{{ ref('stg_st_base') }} AS sb
LEFT OUTER JOIN
	{{ ref('stg_st_identification_documents') }} AS sid
ON
	sid.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	{{ ref('stg_st_other_names') }}  AS son
ON
	son.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	{{ ref('stg_st_personal_identification_documents') }}  AS spid
ON
	spid.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	{{ ref('stg_st_visas') }}  AS sv
ON
	sv.studentuniqueid = sb.studentuniqueid

)

select * from final