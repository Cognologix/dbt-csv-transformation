WITH st_identification_documents AS (
	SELECT
		{{ var('LOADID',-1) }} as LOADID,
		sid.studentuniqueid,
		TRIM(sid.identificationdocumentusedescriptor) AS identificationdocumentusedescriptor,
		TRIM(sid.personalinformationverificationdescriptor) AS personalinformationverificationdescriptor,
		TRIM(sid.issuercountrydescriptor) AS issuercountrydescriptor,
		sid.documentexpirationdate,
		TRIM(sid.documenttitle) AS documenttitle,
		TRIM(sid.issuerdocumentidentificationcode) AS issuerdocumentidentificationcode,
		TRIM(sid.issuername) AS issuername

	FROM
		{{ source('raw_data', 'student_identification_documents')}} AS sid
	WHERE
	    studentuniqueid IS NOT NULL AND
	    NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(personalinformationverificationdescriptor),'') IS NOT NULL
)

select * from st_identification_documents
