WITH st_personal_identification_documents AS (
	SELECT
		spid.studentuniqueid,
        TRIM(spid.identificationdocumentusedescriptor) AS identificationdocumentusedescriptor,
		TRIM(spid.personalinformationverificationdescriptor) AS personalinformationverificationdescriptor,
		TRIM(spid.issuercountrydescriptor) AS issuercountrydescriptor,
		spid.documentexpirationdate,
		TRIM(spid.documenttitle) AS documenttitle,
		TRIM(spid.issuerdocumentidentificationcode) AS issuerdocumentidentificationcode,
		TRIM(spid.issuername) AS issuername

	FROM
		{{ source('public', 'student_personal_identification_documents')}} AS spid
    WHERE
	    studentuniqueid IS NOT NULL
	    AND NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NOT NULL
	    AND NULLIF(TRIM(personalinformationverificationdescriptor),'') IS NOT NULL
)

select * from st_personal_identification_documents