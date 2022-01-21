WITH st_identification_documents AS (
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
		{{ source('public', 'student_identification_documents')}} AS sid
	GROUP BY
		sid.studentuniqueid
)

select * from st_identification_documents;
