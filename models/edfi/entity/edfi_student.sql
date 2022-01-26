WITH st_base AS (
	SELECT
		CAST( sb.studentuniqueid AS TEXT ) AS studentuniqueid,
		uuid_generate_v4() AS resourceid, 
		json_build_object(
			'studentuniqueid', CAST( sb.studentuniqueid AS TEXT )
		)AS externalid,
		'STUDENT' AS resourcetype,
		sb.operation,
		0 AS status,
		json_build_object(
			'personId', CAST( sb.personid AS TEXT ),
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
),
census_block_groups AS 
(
	SELECT
		scbg.studentuniqueid,
		jsonb_agg(json_build_object(
				'txStudentCensusBlockGroup', scbg.tx_studentcensusblockgroup,
				'txBeginDate', scbg.tx_begindate,
				'txEndDate', scbg.tx_enddate
			)
		) AS censusBlockGroups
	FROM
		public.student_census_block_groups AS scbg
	GROUP BY
			scbg.studentuniqueid
),
crisis_events AS (
	SELECT
		sce.studentuniqueid,
		jsonb_agg(json_build_object(
				'txCrisisEventDescriptor', sce.tx_crisiseventdescriptor,
				'txBeginDate', sce.tx_begindate,
				'txEndDate', sce.tx_enddate
			)
		) AS crisisEvents
	FROM
		public.student_crisis_events AS sce
	GROUP BY
		sce.studentuniqueid
),
extensions AS
(
	SELECT 
		CAST( sb.studentuniqueid AS TEXT ),
		json_build_object(
			'TexasExtensions',
			json_build_object(
				'txAdultPreviousAttendanceIndicator', sb.tx_adultpreviousattendanceindicator,
				'txLocalStudentID', sb.tx_localstudentid,
				'txStudentID', sb.tx_studentid,
				'censusBlockGroups', scbg.censusBlockGroups,
				'crisisEvents', sce.crisisEvents
			)
		) AS _ext
	FROM
		st_base as sb
	LEFT JOIN 
		census_block_groups As scbg
	ON
		CAST ( scbg.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
	LEFT JOIN 
		crisis_events As sce
	ON
		CAST ( sce.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
)

SELECT
	resourceid,
	externalid,
	resourcetype,
	operation,
	json_build_object(
		'studentUniqueId', CAST( sb.studentuniqueid AS TEXT ),
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
		'visas', sv.visas,
		'_ext', extensions._ext
	) AS payload,
	status
FROM
	st_base AS sb
LEFT OUTER JOIN
	st_identification_documents AS sid
ON 
	CAST ( sid.studentuniqueid AS TEXT) = CAST( sb.studentuniqueid AS TEXT )
LEFT OUTER JOIN
	st_other_names AS son
ON 
	CAST ( son.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
LEFT OUTER JOIN
	st_personal_identification_documents AS spid
ON 
	CAST ( spid.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
LEFT OUTER JOIN
	st_visas AS sv
ON 
	CAST ( sv.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
LEFT OUTER JOIN
	extensions
ON
	CAST ( extensions.studentuniqueid AS TEXT ) = CAST( sb.studentuniqueid AS TEXT )
