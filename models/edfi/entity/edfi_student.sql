CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

WITH st_base AS (
	SELECT
		sb.operation,
		sb.studentuniqueid,
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
		sb.personaltitleprefix
	FROM 
		public.student_base AS sb
),
st_identification_documents AS (
	SELECT
		sid.studentuniqueid,
		jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', sid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', sid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', sid.issuercountrydescriptor,
				'documentExpirationDate', sid.documentexpirationdate,
				'documentTitle', sid.documenttitle,
				'issuerDocumentIdentificationCode', sid.issuerdocumentidentificationcode,
				'issuerName', sid.issuername
			)
		) AS identificationDocuments
	FROM 
		public.student_identification_documents AS sid
	GROUP BY
		sid.studentuniqueid
),
st_other_names AS (
	SELECT 
		son.studentuniqueid,
		jsonb_agg(json_build_object(
				'otherNameTypeDescriptor', son.othernametypedescriptor,
				'firstName', son.firstname,
				'generationCodeSuffix', son.generationcodesuffix,
				'lastSurname', son.lastsurname,
				'middleName', son.middlename,
				'personalTitlePrefix', son.personaltitleprefix
			)
		) AS otherNames
	FROM 
		public.student_other_names AS son
	GROUP BY
		son.studentuniqueid
),
st_personal_identification_documents AS (
	SELECT 
		spid.studentuniqueid,
        jsonb_agg(json_build_object(	
				'identificationDocumentUseDescriptor', spid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', spid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', spid.issuercountrydescriptor,
				'documentExpirationDate', spid.documentexpirationdate,
				'documentTitle', spid.documenttitle,
				'issuerDocumentIdentificationCode', spid.issuerdocumentidentificationcode,
				'issuerName', spid.issuername
			)
		) AS personalIdentificationDocuments
	FROM 
		public.student_personal_identification_documents AS spid
	GROUP BY
		spid.studentuniqueid
),
st_visas AS (
	SELECT 
		sv.studentuniqueid,
		jsonb_agg(json_build_object(
				'visaDescriptor', sv.visadescriptor 
			)
		) AS visas
	FROM 
		public.student_visas AS sv
	GROUP BY
		sv.studentuniqueid
)

SELECT
	uuid_generate_v4() AS uniqueid, 
	LPAD(sb.studentuniqueid::text, 6, '0') AS resourceid,
	'STUDENT' AS resourcetype,
	sb.operation,
	0 AS status,
	json_build_object(
		'studentUniqueId', sb.studentuniqueid,
		'identificationDocuments', sb.identificationdocuments,
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
	) AS payload
FROM
	st_base AS sb
LEFT OUTER JOIN
	st_identification_documents AS sid
ON 
	sid.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	st_other_names AS son
ON 
	son.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	st_personal_identification_documents AS spid
ON 
	spid.studentuniqueid = sb.studentuniqueid
LEFT OUTER JOIN
	st_visas AS sv
ON 
	sv.studentuniqueid = sb.studentuniqueid
